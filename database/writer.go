package database

import (
	"context"
	"sync"

	"github.com/activecm/rita/config"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	// BulkChange represents mgo upserts, updates, and removals
	BulkChange struct {
		Selector  interface{} // The selector document
		Update    interface{} // The update document if updating the document
		Upsert    bool        // Whether to insert in case the document isn't found
		Remove    bool        // Whether to remove the document found rather than updating
		SelectAll bool        // Whether to use RemoveAll/ UpdateAll
		ctx       context.Context
	}

	// BulkChanges is a map of collections to the changes that should be applied to each one
	BulkChanges map[string][]BulkChange

	// MgoBulkWriter is a pipeline worker which properly batches bulk updates for MongoDB
	MgoBulkWriter struct {
		db           *DB              // provides access to MongoDB
		conf         *config.Config   // contains details needed to access MongoDB
		log          *log.Logger      // main logger for RITA
		writeChannel chan BulkChanges // holds analyzed data
		writeWg      *sync.WaitGroup  // wait for writing to finish
		writerName   string           // used in error reporting
		unordered    bool             // if the operations can be applied in any order, MongoDB can run the updates in parallel
		maxBulkCount int              // max number of changes to include in each bulk update
		maxBulkSize  int              // max total size of BSON documents making up each bulk update
	}
)

// Size serializes the changes to BSON using provided buffer and returns total size
// of the BSON description of the changes. Note this method slightly underestimates the
// total amount BSON needed to describe the changes since extra flags may be sent along.
func (m BulkChange) Size(buffer []byte) ([]byte, int) {
	size := 0
	if len(buffer) > 0 { // in case the byte slice has something in it already
		buffer = buffer[:0]
	}

	if m.Selector != nil {
		buffer, _ = bson.Marshal(m.Selector)
		size += len(buffer)
		buffer = buffer[:0]
	}
	if m.Update != nil {
		buffer, _ = bson.Marshal(m.Update)
		size += len(buffer)
		buffer = buffer[:0]
	}
	return buffer, size
}

// Apply adds the change described to a bulk buffer
func (m BulkChange) Apply(bulk *mongo.Collection) {
	if m.Selector == nil {
		return // can't describe a change without a selector
	}

	if m.Remove && m.SelectAll {
		bulk.DeleteMany(m.ctx, m.Selector)
	} else if m.Remove /*&& !m.SelectAll*/ {
		bulk.DeleteOne(m.ctx, m.Selector)
	} else if m.Update != nil && m.Upsert {
		opts := options.Update().SetUpsert(true)
		bulk.UpdateOne(m.ctx, m.Selector, m.Update, opts)
	} else if m.Update != nil && m.SelectAll {
		opts := options.Update().SetUpsert(true)
		bulk.UpdateMany(m.ctx, m.Selector, m.Update, opts)
	} else if m.Update != nil /*&& !m.Upsert && !m.SelectAll*/ {
		bulk.UpdateOne(m.ctx, m.Selector, m.Update)
	}
}

// NewBulkWriter creates a new writer object to write output data to collections
func NewBulkWriter(db *DB, conf *config.Config, log *log.Logger, unorderedWritesOK bool, writerName string) *MgoBulkWriter {
	return &MgoBulkWriter{
		db:           db,
		conf:         conf,
		log:          log,
		writeChannel: make(chan BulkChanges),
		writeWg:      new(sync.WaitGroup),
		writerName:   writerName,
		unordered:    unorderedWritesOK,
		// Cap the bulk buffers at 500 updates. 1000 should theoretically work, but we've run into issues in the past, so we halved it.
		maxBulkCount: 500,
		// Cap the bulk buffers at 15MB. This cap ensures that our bulk transactions don't exceed the 16MB limit imposed on MongoDB docs/ operations.
		maxBulkSize: 15 * 1000 * 1000,
	}
}

// Collect sends a group of results to the writer for writing out to the database
func (w *MgoBulkWriter) Collect(data BulkChanges) {
	w.writeChannel <- data
}

// close waits for the write threads to finish
func (w *MgoBulkWriter) Close() {
	close(w.writeChannel)
	w.writeWg.Wait()
}

// start kicks off a new write thread
func (w *MgoBulkWriter) Start() {
	w.writeWg.Add(1)
	go func() {
		bulkBuffers := map[string]*mongo.Collection{} // stores a mgo.Bulk buffer for each collection
		bulkBufferSizes := map[string]int{}           // stores the size in bytes of the BSON documents in each mgo.Bulk buffer
		bulkBufferLengths := map[string]int{}         // stores the number of changes stored in each mgo.Bulk buffer
		var sizeBuffer []byte                         // used (and re-used) for BSON serialization in order to calculate the size of each BSON doc
		var changeSize int                            // holds the total size of each BSON serialized change before being added to bulkBufferSizes

		for data := range w.writeChannel { // process data as it streams into the writer
			for tgtColl, bulkChanges := range data { // loop through each collection that needs updated

				// initialize/ grab the bulk buffer associated with this collection
				bulkBuffer, bufferExists := bulkBuffers[tgtColl]
				if !bufferExists {
					bulkBuffer = w.db.Client.Database(w.db.GetSelectedDB()).Collection(tgtColl)
					//TODO(fryy): Bulk writes ?
					/*if w.unordered {
						// if the order in which the updates occur doesn't matter, allow MongoDB to apply the updates in parallel
						bulkBuffer.Unordered()
					}*/
					bulkBuffers[tgtColl] = bulkBuffer
				}

				for _, change := range bulkChanges { // loop through each change that needs to be applied to the collection
					sizeBuffer, changeSize = change.Size(sizeBuffer)

					// if the bulk buffer has already reached the max number of changes or
					// if the total size of the bulk buffer would exceed the max size after inserting the current change
					// run the existing bulk buffer against MongoDB
					if bulkBufferLengths[tgtColl] >= w.maxBulkCount || bulkBufferSizes[tgtColl]+changeSize >= w.maxBulkSize {
						/*info, err := bulkBuffer.Run()
						if err != nil {
							w.log.WithFields(log.Fields{
								"Module":     w.writerName,
								"Collection": tgtColl,
								"Info":       info,
							}).Error(err)
						}*/
						// make sure to reset the stats we are tracking about the bulk buffer
						bulkBufferLengths[tgtColl] = 0
						bulkBufferSizes[tgtColl] = 0
					}

					// insert the change into the bulk buffer and update the stats we are tracking about the bulk buffer
					change.Apply(bulkBuffer)
					bulkBufferLengths[tgtColl]++
					bulkBufferSizes[tgtColl] += changeSize
				}
			}
		}

		// after the writer is done receiving inputs, make sure to drain all of the buffers before exiting
		for tgtColl, _ := range bulkBuffers {
			/*info, err := bulkBuffer.Run()
			if err != nil {
				w.log.WithFields(log.Fields{
					"Module":     w.writerName,
					"Collection": tgtColl,
					"Info":       info,
				}).Error(err)
			}*/

			bulkBufferLengths[tgtColl] = 0
			bulkBufferSizes[tgtColl] = 0
		}
		w.writeWg.Done()
	}()
}
