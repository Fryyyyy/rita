package hostname

import (
	"context"
	"strings"
	"sync"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	log "github.com/sirupsen/logrus"
)

type (
	//analyzer : structure for exploded dns analysis
	analyzer struct {
		chunk            int                        //current chunk (0 if not on rolling analysis)
		db               *database.DB               // provides access to MongoDB
		conf             *config.Config             // contains details needed to access MongoDB
		log              *log.Logger                // logger for writing out errors and warnings
		analyzedCallback func(database.BulkChanges) // called on each analyzed result
		closedCallback   func()                     // called when .close() is called and no more calls to analyzedCallback will be made
		analysisChannel  chan *Input                // holds unanalyzed data
		analysisWg       sync.WaitGroup             // wait for analysis to finish
	}
)

// newAnalyzer creates a new collector for parsing hostnames
func newAnalyzer(chunk int, db *database.DB, conf *config.Config, log *log.Logger, analyzedCallback func(database.BulkChanges), closedCallback func()) *analyzer {
	return &analyzer{
		chunk:            chunk,
		db:               db,
		conf:             conf,
		log:              log,
		analyzedCallback: analyzedCallback,
		closedCallback:   closedCallback,
		analysisChannel:  make(chan *Input),
	}
}

// collect sends a group of domains to be analyzed
func (a *analyzer) collect(data *Input) {
	a.analysisChannel <- data
}

// close waits for the collector to finish
func (a *analyzer) close() {
	close(a.analysisChannel)
	a.analysisWg.Wait()
	a.closedCallback()
}

// start kicks off a new analysis thread
func (a *analyzer) start() {
	a.analysisWg.Add(1)
	go func() {
		for datum := range a.analysisChannel {

			// in some of these strings, the empty space will get counted as a domain,
			// this was an issue in the old version of exploded dns and caused inaccuracies
			if (datum.Host == "") || (strings.HasSuffix(datum.Host, "in-addr.arpa")) {
				continue
			}

			mainUpdate := mainQuery(datum, a.chunk)

			blUpdate, err := blQuery(a.db.Context, datum, a.db.Client, a.conf.S.Blacklisted.BlacklistDatabase) // TODO: Move to BL package
			if err != nil {
				a.log.WithFields(log.Fields{
					"Module": "hostname",
					"Data":   datum.Host,
				}).Error(err)
			}

			totalUpdate := database.MergeBSONMaps(mainUpdate, blUpdate)

			a.analyzedCallback(database.BulkChanges{
				a.conf.T.DNS.HostnamesTable: []database.BulkChange{{
					Selector: bson.M{"host": datum.Host},
					Update:   totalUpdate,
					Upsert:   true,
				}},
			})
		}

		a.analysisWg.Done()
	}()
}

// mainQuery records the IPs which the hostname resolved to and the IPs which
// queried for the the hostname
func mainQuery(datum *Input, chunk int) bson.M {
	return bson.M{
		"$set": bson.M{
			"cid": chunk,
		},

		"$push": bson.M{
			"dat": bson.M{
				"$each": []bson.M{{
					"ips":     datum.ResolvedIPs.Items(),
					"src_ips": datum.ClientIPs.Items(),
					"cid":     chunk,
				}},
			},
		},
	}
}

// blQuery marks the given hostname as blacklisted or not
func blQuery(ctx context.Context, datum *Input, ssn *mongo.Client, blDB string) (bson.M, error) {
	// check if blacklisted destination
	blCount, err := ssn.Database(blDB).Collection("hostname").CountDocuments(ctx, bson.M{"index": datum.Host})
	blacklisted := blCount > 0

	return bson.M{
		"$set": bson.M{
			"blacklisted": blacklisted,
		},
	}, err
}
