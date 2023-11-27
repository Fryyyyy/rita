package uconn

import (
	"fmt"
	"runtime"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	"github.com/activecm/rita/pkg/data"
	"github.com/activecm/rita/pkg/host"
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

// NewMongoRepository bundles the given resources for updating MongoDB with unique connection data
func NewMongoRepository(db *database.DB, conf *config.Config, logger *log.Logger) Repository {
	return &repo{
		database: db,
		config:   conf,
		log:      logger,
	}
}

// CreateIndexes creates indexes for the uconn collection
func (r *repo) CreateIndexes() error {
	// set collection name
	collectionName := r.config.T.Structure.UniqueConnTable

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
				{Key: "src", Value: 1},
				{Key: "dst", Value: 1},
				{Key: "src_network_uuid", Value: 1},
				{Key: "dst_network_uuid", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{
				{Key: "src", Value: 1},
				{Key: "src_network_uuid", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "dst", Value: 1},
				{Key: "dst_network_uuid", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "dat.count", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "dat.maxdur", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "strobe", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "count", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "tbytes", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "tdur", Value: 1},
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

// Upsert records the given unique connection data in MongoDB. Summaries are
// created for the given local hosts in MongoDB.
func (r *repo) Upsert(uconnMap map[string]*Input, hostMap map[string]*host.Input) {
	// Phase 1: Analysis

	// Create the workers for analysis
	writerWorker := database.NewBulkWriter(r.database, r.config, r.log, true, "uconn")

	analyzerWorker := newAnalyzer(
		r.config.S.Rolling.CurrentChunk,
		int64(r.config.S.Strobe.ConnectionLimit),
		r.database,
		r.log,
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
	bar := p.AddBar(int64(len(uconnMap)),
		mpb.PrependDecorators(
			decor.Name("\t[-] Unique Connection Analysis:", decor.WC{W: 30, C: decor.DidentRight}),
			decor.CountersNoUnit(" %d / %d ", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)

	// loop over map entries
	for _, entry := range uconnMap {
		analyzerWorker.collect(entry)
		bar.IncrBy(1)
	}
	p.Wait()

	// start the closing cascade (this will also close the other channels)
	analyzerWorker.close()

	// Phase 2: Summary

	// grab the local hosts we have seen during the current analysis period
	var localHosts []data.UniqueIP
	for _, entry := range hostMap {
		if entry.IsLocal {
			localHosts = append(localHosts, entry.Host)
		}
	}

	// skip the summarize phase if there are no local hosts to summarize
	if len(localHosts) == 0 {
		fmt.Println("\t[!] Skipping Unique Connection Aggregation: No Internal Hosts")
		return
	}

	// initialize a new writer for the summarizer
	writerWorker = database.NewBulkWriter(r.database, r.config, r.log, true, "uconn")
	summarizerWorker := newSummarizer(
		r.config.S.Rolling.CurrentChunk,
		r.database,
		r.config,
		r.log,
		writerWorker.Collect,
		writerWorker.Close,
	)

	// kick off the threaded goroutines
	for i := 0; i < util.Max(1, runtime.NumCPU()/2); i++ {
		summarizerWorker.start()
		writerWorker.Start()
	}

	// add a progress bar for troubleshooting
	p = mpb.New(mpb.WithWidth(20))
	bar = p.AddBar(int64(len(localHosts)),
		mpb.PrependDecorators(
			decor.Name("\t[-] Unique Connection Aggregation:", decor.WC{W: 30, C: decor.DidentRight}),
			decor.CountersNoUnit(" %d / %d ", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)

	// loop over the local hosts that need to be summarized
	for _, localHost := range localHosts {
		summarizerWorker.collect(localHost)
		bar.IncrBy(1)
	}

	p.Wait()

	// start the closing cascade (this will also close the other channels)
	summarizerWorker.close()
}
