package database

import (
	"context"
	"fmt"
	"os"

	"github.com/activecm/rita/config"
	"github.com/blang/semver"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	log "github.com/sirupsen/logrus"
)

// MinMongoDBVersion is the lower, inclusive bound on the
// versions of MongoDB compatible with RITA
var MinMongoDBVersion = semver.Version{
	Major: 4,
	Minor: 2,
	Patch: 0,
}

// MaxMongoDBVersion is the upper, exclusive bound on the
// versions of MongoDB compatible with RITA
var MaxMongoDBVersion = semver.Version{
	Major: 4,
	Minor: 3,
	Patch: 0,
}

// DB is the workhorse container for messing with the database
type DB struct {
	Client   *mongo.Client
	Context  context.Context
	log      *log.Logger
	selected string
}

// NewDB constructs a new DB struct
func NewDB(conf *config.Config, log *log.Logger) (*DB, error) {
	// Jump into the requested database
	//TODO(fryy): Context?
	ctx := context.TODO()
	client, err := connectToMongoDB(ctx, conf, log)
	if err != nil {
		return nil, err
	}

	return &DB{
		Client:   client,
		Context:  ctx,
		log:      log,
		selected: "",
	}, nil
}

// connectToMongoDB connects to MongoDB possibly with authentication and TLS
func connectToMongoDB(ctx context.Context, conf *config.Config, logger *log.Logger) (*mongo.Client, error) {
	connString := conf.S.MongoDB.ConnectionString
	//authMechanism := conf.R.MongoDB.AuthMechanismParsed
	//tlsConfig := conf.R.MongoDB.TLS.TLSConfig

	var client *mongo.Client
	var err error
	clientOpts := options.Client().ApplyURI(connString)
	clientOpts.SetSocketTimeout(conf.S.MongoDB.SocketTimeout)
	client, err = mongo.Connect(ctx, clientOpts)

	if err != nil {
		return client, err
	}

	fmt.Fprintf(os.Stdout, "End of Connect")
	return client, nil

}

// SelectDB selects a database for analysis
func (d *DB) SelectDB(db string) {
	d.selected = db
}

// GetSelectedDB retrieves the currently selected database for analysis
func (d *DB) GetSelectedDB() string {
	return d.selected
}

// CollectionExists returns true if collection exists in the currently
// selected database
func (d *DB) CollectionExists(table string) bool {
	coll, err := d.Client.Database(d.selected).ListCollectionNames(d.Context, bson.D{})
	if err != nil {
		d.log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("Failed collection name lookup")
		return false
	}
	for _, name := range coll {
		if name == table {
			return true
		}
	}
	return false
}

// CreateCollection creates a new collection in the currently selected
// database with the required indexes
func (d *DB) CreateCollection(name string, indexes []mongo.IndexModel) error {
	d.log.Debug("Building collection: ", name, " with ", len(indexes), " indexes")

	err := d.Client.Database(d.selected).CreateCollection(d.Context, name)

	// Make sure it actually got created
	if err != nil {
		return err
	}

	//TODO(fryy): Cursor close at every point
	//TODO(fryy): Put back in Allow Disk Usage on pipelines.
	collection := d.Client.Database(d.selected).Collection(name)
	_, err = collection.Indexes().CreateMany(d.Context, indexes)
	if err != nil {
		return err
	}
	return nil
}

// AggregateCollection builds a collection via a MongoDB pipeline
func (d *DB) AggregateCollection(sourceCollection string, pipeline []bson.D) (*mongo.Cursor, error) {

	// Identify the source collection we will aggregate information from into the new collection
	if !d.CollectionExists(sourceCollection) {
		d.log.Warning("Failed aggregation: (Source collection: ",
			sourceCollection, " doesn't exist)")
		return nil, fmt.Errorf("Collection doesn't exist")
	}
	collection := d.Client.Database(d.selected).Collection(sourceCollection)

	// Create the pipe
	opts := options.Aggregate().SetAllowDiskUse(true)
	pipe, err := collection.Aggregate(d.Context, pipeline, opts)
	if err != nil {
		return pipe, err
	}

	// If error, Throw computer against wall and drink 2 angry beers while
	// questioning your life, purpose, and relationships.
	if pipe.Err() != nil {
		d.log.WithFields(log.Fields{
			"error": pipe.Err().Error(),
		}).Error("Failed aggregate operation")
		return nil, pipe.Err()
	}
	return pipe, nil
}

// MergeBSONMaps recursively merges several bson.M objects into a single map.
// When merging slices of maps with the same associated key, the slices are concatenated.
// If two or more maps define the same key and they are not both bson.M objects,
// a panic occurs. It should be known ahead of time whether keys will conflict
// before calling this function.
func MergeBSONMaps(maps ...bson.M) bson.M {
	result := bson.M{}
	for _, mapToMerge := range maps {
		for keyToMerge, valueToMerge := range mapToMerge {
			// handle new keys
			currVal, currValExists := result[keyToMerge]
			if !currValExists {
				result[keyToMerge] = valueToMerge
				continue
			}

			// handle merging child maps
			currValMap, currValIsMap := currVal.(bson.M)
			mapToMerge, valueToMergeIsMap := valueToMerge.(bson.M)
			if currValIsMap && valueToMergeIsMap {
				result[keyToMerge] = MergeBSONMaps(currValMap, mapToMerge)
				continue
			}

			// handle merging arrays of maps
			currValMapSlice, currValIsMapSlice := currVal.([]bson.M)
			mapSliceToMerge, valueToMergeIsMapSlice := valueToMerge.([]bson.M)
			if currValIsMapSlice && valueToMergeIsMapSlice {
				result[keyToMerge] = append(currValMapSlice, mapSliceToMerge...)
				continue
			}

			// maps cannot be merged due to a type mismatch or overwriting issue
			panic(fmt.Sprintf(
				"BSON maps could not be merged due to conflicting key value pairs:\n"+
					"\tKey: %s\n\tValue 1: %s\n\tValue 2: %s\n",
				keyToMerge, currVal, valueToMerge,
			))
			//return nil
		}
	}

	return result
}
