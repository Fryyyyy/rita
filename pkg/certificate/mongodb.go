package certificate

import (
	"runtime"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	"github.com/activecm/rita/util"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	log "github.com/sirupsen/logrus"
)

type repo struct {
	database *database.DB
	config   *config.Config
	log      *log.Logger
}

// NewMongoRepository bundles the given resources for updating MongoDB with invalid certificate data
func NewMongoRepository(db *database.DB, conf *config.Config, logger *log.Logger) Repository {
	return &repo{
		database: db,
		config:   conf,
		log:      logger,
	}
}

// CreateIndexes creates indexes for the certificate collection
func (r *repo) CreateIndexes() error {
	// set collection name
	collectionName := r.config.T.Cert.CertificateTable

	// check if collection already exists
	names, _ := r.database.Client.Database(r.database.GetSelectedDB()).ListCollectionNames(r.database.Context, bson.D{})

	// if collection exists, we don't need to do anything else
	for _, name := range names {
		if name == collectionName {
			return nil
		}
	}

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
				{"dat.seen", 1},
			},
		},
	}

	// create collection
	err := r.database.CreateCollection(collectionName, indexes)
	if err != nil {
		return err
	}

	return nil
}

// Upsert records the given certificate data in MongoDB
func (r *repo) Upsert(certMap map[string]*Input) {
	// Create the workers
	writerWorker := database.NewBulkWriter(r.database, r.config, r.log, true, "certificate")

	analyzerWorker := newAnalyzer(
		r.config.S.Rolling.CurrentChunk,
		r.database,
		r.config,
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
	bar := p.AddBar(int64(len(certMap)),
		mpb.PrependDecorators(
			decor.Name("\t[-] Invalid Cert Analysis:", decor.WC{W: 30, C: decor.DidentRight}),
			decor.CountersNoUnit(" %d / %d ", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)

	// loop over map entries
	for _, value := range certMap {
		analyzerWorker.collect(value)
		bar.IncrBy(1)
	}

	p.Wait()

	// start the closing cascade (this will also close the other channels)
	analyzerWorker.close()
}
