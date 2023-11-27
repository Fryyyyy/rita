package host

import (
	"runtime"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	"github.com/activecm/rita/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"

	log "github.com/sirupsen/logrus"
)

type repo struct {
	database *database.DB
	config   *config.Config
	log      *log.Logger
}

// NewMongoRepository bundles the given resources for updating MongoDB with host data
func NewMongoRepository(db *database.DB, conf *config.Config, logger *log.Logger) Repository {
	return &repo{
		database: db,
		config:   conf,
		log:      logger,
	}
}

// CreateIndexes creates indexes for the host collection
func (r *repo) CreateIndexes() error {
	coll := r.database.Client.Database(r.database.GetSelectedDB()).Collection(r.config.T.Structure.HostTable)

	// create hosts collection
	// Desired indexes
	indexes := []mongo.IndexModel{
		mongo.IndexModel{
			Keys: bson.D{
				{"ip", 1},
				{"network_uuid", 1},
			},
			Options: options.Index().SetUnique(true),
		},
		mongo.IndexModel{
			Keys: bson.D{
				{"local", 1},
			},
		},
		mongo.IndexModel{
			Keys: bson.D{
				{"ipv4_binary", 1},
			},
		},
		mongo.IndexModel{
			Keys: bson.D{
				{"dat.mdip.ip", 1},
				{"dat.mdip.network_uuid", 1},
			},
		},
		mongo.IndexModel{
			Keys: bson.D{
				{"dat.mbdst.ip", 1},
				{"dat.mbdst.network_uuid", 1},
			},
		},
		mongo.IndexModel{
			Keys: bson.D{
				{"dat.mbproxy", 1},
			},
		},
	}

	_, err := coll.Indexes().CreateMany(r.database.Context, indexes)
	return err
}

// Upsert records the given host data in MongoDB
func (r *repo) Upsert(hostMap map[string]*Input) {

	// 1st Phase: Analysis

	// Create the workers
	writerWorker := database.NewBulkWriter(r.database, r.config, r.log, true, "host")

	analyzerWorker := newAnalyzer(
		r.config.S.Rolling.CurrentChunk,
		r.config,
		r.database,
		r.log,
		writerWorker.Collect,
		writerWorker.Close,
	)

	// kick off the threaded goroutines
	for i := 0; i < util.Max(1, runtime.NumCPU()/2); i++ {
		analyzerWorker.start()
		writerWorker.Start()
	}

	// progress bar for troubleshooting
	p := mpb.New(mpb.WithWidth(20))
	bar := p.AddBar(int64(len(hostMap)),
		mpb.PrependDecorators(
			decor.Name("\t[-] Host Analysis:", decor.WC{W: 30, C: decor.DidentRight}),
			decor.CountersNoUnit(" %d / %d ", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)

	// loop over map entries
	for _, entry := range hostMap {
		analyzerWorker.collect(entry)
		bar.IncrBy(1)
	}
	p.Wait()

	// start the closing cascade (this will also close the other channels)
	analyzerWorker.close()
}
