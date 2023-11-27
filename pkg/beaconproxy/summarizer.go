package beaconproxy

import (
	"context"
	"sync"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	"github.com/activecm/rita/pkg/data"
	"github.com/globalsign/mgo"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type (
	//summarizer records summary data for individual hosts using proxy beacon data
	summarizer struct {
		chunk              int                        // current chunk (0 if not on rolling summary)
		db                 *database.DB               // provides access to MongoDB
		conf               *config.Config             // contains details needed to access MongoDB
		log                *log.Logger                // main logger for RITA
		summarizedCallback func(database.BulkChanges) // called on each summarized result
		closedCallback     func()                     // called when .close() is called and no more calls to summarizedCallback will be made
		summaryChannel     chan data.UniqueIP         // holds unsummarized data
		summaryWg          sync.WaitGroup             // wait for summary to finish
	}
)

// newSummarizer creates a new summarizer for proxy beacon data
func newSummarizer(chunk int, db *database.DB, conf *config.Config, log *log.Logger, summarizedCallback func(database.BulkChanges), closedCallback func()) *summarizer {
	return &summarizer{
		chunk:              chunk,
		db:                 db,
		conf:               conf,
		log:                log,
		summarizedCallback: summarizedCallback,
		closedCallback:     closedCallback,
		summaryChannel:     make(chan data.UniqueIP),
	}
}

// collect collects an internal host to create summary data for
func (s *summarizer) collect(datum data.UniqueIP) {
	s.summaryChannel <- datum
}

// close waits for the summarizer to finish
func (s *summarizer) close() {
	close(s.summaryChannel)
	s.summaryWg.Wait()
	s.closedCallback()
}

// start kicks off a new summary thread
func (s *summarizer) start() {
	s.summaryWg.Add(1)
	go func() {
		for datum := range s.summaryChannel {
			proxyBeaconCollection := s.db.Client.Database(s.db.GetSelectedDB()).Collection(s.conf.T.BeaconProxy.BeaconProxyTable)
			hostCollection := s.db.Client.Database(s.db.GetSelectedDB()).Collection(s.conf.T.Structure.HostTable)

			maxProxyBeaconSelector, maxProxyBeaconQuery, err := maxProxyBeaconUpdate(s.db.Context,
				datum, proxyBeaconCollection, hostCollection, s.chunk,
			)
			if err != nil {
				if err != mgo.ErrNotFound {
					s.log.WithFields(log.Fields{
						"Module": "beaconsProxy",
						"Data":   datum,
					}).Error(err)
				}
				continue
			}

			if len(maxProxyBeaconQuery) > 0 {
				s.summarizedCallback(database.BulkChanges{
					s.conf.T.Structure.HostTable: []database.BulkChange{{
						Selector: maxProxyBeaconSelector,
						Update:   maxProxyBeaconQuery,
						Upsert:   true,
					}},
				})
			}
		}
		s.summaryWg.Done()
	}()
}

// maxProxyBeaconUpdate finds the highest scoring proxy beacon from this import session for a particular host
func maxProxyBeaconUpdate(ctx context.Context, datum data.UniqueIP, beaconProxyColl, hostColl *mongo.Collection, chunk int) (bson.M, bson.M, error) {

	var maxBeaconProxy struct {
		Fqdn  string  `bson:"fqdn"`
		Score float64 `bson:"score"`
	}

	mbdstQuery := maxProxyBeaconPipeline(datum)
	cursor, err := beaconProxyColl.Aggregate(ctx, mbdstQuery)
	if err != nil {
		return nil, nil, err
	}

	if cursor.Next(ctx) {
		if err = cursor.Decode(&maxBeaconProxy); err != nil {
			panic(err)
		}
	}

	hostSelector := datum.BSONKey()
	hostWithDatEntrySelector := database.MergeBSONMaps(
		hostSelector,
		bson.M{"dat": bson.M{"$elemMatch": bson.M{"mbproxy": bson.M{"$exists": true}}}},
	)

	nExistingEntries, err := hostColl.CountDocuments(ctx, hostWithDatEntrySelector)
	if err != nil {
		return nil, nil, err
	}

	if nExistingEntries > 0 {
		updateQuery := bson.M{
			"$set": bson.M{
				"dat.$.mbproxy":                maxBeaconProxy.Fqdn,
				"dat.$.max_beacon_proxy_score": maxBeaconProxy.Score,
				"dat.$.cid":                    chunk,
			},
		}
		return hostWithDatEntrySelector, updateQuery, nil
	}

	insertQuery := bson.M{
		"$push": bson.M{
			"dat": bson.M{
				"$each": []bson.M{{
					"mbproxy":                maxBeaconProxy.Fqdn,
					"max_beacon_proxy_score": maxBeaconProxy.Score,
					"cid":                    chunk,
				}},
			},
		},
	}

	return hostSelector, insertQuery, nil
}

func maxProxyBeaconPipeline(host data.UniqueIP) []bson.M {
	return []bson.M{
		{"$match": bson.M{
			"src":              host.IP,
			"src_network_uuid": host.NetworkUUID,
		}},
		// drop unnecessary data
		{"$project": bson.M{
			"fqdn":  1,
			"score": 1,
		}},
		// find the peer with the maximum score
		{"$sort": bson.M{
			"score": -1,
		}},
		{"$limit": 1},
	}
}
