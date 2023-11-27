package blacklist

import (
	"runtime"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	"github.com/activecm/rita/pkg/data"
	"github.com/activecm/rita/util"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	log "github.com/sirupsen/logrus"
)

type repo struct {
	database *database.DB
	config   *config.Config
	log      *log.Logger
}

// NewMongoRepository bundles the given resources for updating MongoDB with threat intel data
func NewMongoRepository(db *database.DB, conf *config.Config, logger *log.Logger) Repository {
	return &repo{
		database: db,
		config:   conf,
		log:      logger,
	}
}

// CreateIndexes sets up the indices needed to find hosts which contacted unsafe hosts
func (r *repo) CreateIndexes() error {
	coll := r.database.Client.Database(r.database.GetSelectedDB()).Collection(r.config.T.Structure.HostTable)

	// create hosts collection
	// Desired indexes
	index :=
		mongo.IndexModel{
			Keys: bson.D{
				{"dat.bl.ip", 1},
				{"dat.bl.network_uuid", 1},
			},
		}

	_, err := coll.Indexes().CreateOne(r.database.Context, index)
	return err
}

// Upsert creates threat intel records in the host collection for the hosts which
// contacted hosts which have been marked unsafe
func (r *repo) Upsert() {

	// Create the workers
	writerWorker := database.NewBulkWriter(r.database, r.config, r.log, true, "bl_updater")

	analyzerWorker := newAnalyzer(
		r.config.S.Rolling.CurrentChunk,
		r.database,
		r.config,
		r.log,
		writerWorker.Collect,
		writerWorker.Close,
	)

	// kick off the threaded goroutines
	for i := 0; i < util.Max(1, runtime.NumCPU()/2); i++ {
		analyzerWorker.start()
		writerWorker.Start()
	}

	// ensure the worker closing cascade fires when we exit this method
	defer analyzerWorker.close()

	// grab all of the unsafe hosts we have ever seen
	// NOTE: we cannot use the (hostMap map[string]*host.Input)
	// since we are creating peer statistic summaries for the entire
	// observation period not just this import session
	numUnsafeHosts, err := r.database.Client.Database(r.database.GetSelectedDB()).Collection(r.config.T.Structure.HostTable).CountDocuments(r.database.Context, bson.M{"blacklisted": true})

	if err != nil {
		r.log.WithFields(log.Fields{
			"Module": "bl_updater",
		}).Error(err)
	}
	if numUnsafeHosts == 0 {
		// fmt.Println("\t[!] No blacklisted hosts to update")
		return
	}

	// add a progress bar for troubleshooting
	p := mpb.New(mpb.WithWidth(20))
	bar := p.AddBar(int64(numUnsafeHosts),
		mpb.PrependDecorators(
			decor.Name("\t[-] Updating blacklisted peers:", decor.WC{W: 30, C: decor.DidentRight}),
			decor.CountersNoUnit(" %d / %d ", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)

	cursor, err := r.database.Client.Database(r.database.GetSelectedDB()).Collection(r.config.T.Structure.HostTable).Find(r.database.Context, bson.M{"blacklisted": true})
	for cursor.Next(r.database.Context) {
		var unsafeHost data.UniqueIP
		if err := cursor.Decode(&unsafeHost); err != nil {
			panic(err)
		}
		analyzerWorker.collect(unsafeHost)
		bar.IncrBy(1)
	}
	p.Wait()

}
