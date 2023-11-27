package remover

import (
	"fmt"
	"sync"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

type (
	writer struct {
		cid               int            // chuck id for deletion
		db                *database.DB   // provides access to MongoDB
		conf              *config.Config // contains details needed to access MongoDB
		log               *log.Logger    // main logger for RITA
		cidRemoverChannel chan string    // holds target collection names
		updaterChannel    chan update    // holds update queries
		writeWg           sync.WaitGroup // wait for writing to finish
	}
)

// newCIDRemover creates a new writer object to write output data
func newCIDRemover(cid int, db *database.DB, conf *config.Config, log *log.Logger) *writer {
	return &writer{
		cid:               cid,
		db:                db,
		conf:              conf,
		log:               log,
		cidRemoverChannel: make(chan string),
	}
}

// newUpdater creates a new writer object to write output data
func newUpdater(cid int, db *database.DB, conf *config.Config, log *log.Logger) *writer {
	return &writer{
		cid:            cid,
		db:             db,
		conf:           conf,
		log:            log,
		updaterChannel: make(chan update),
	}
}

// collect sends a group of results to the writer for writing out to the database
func (w *writer) collectCIDRemover(data string) {
	w.cidRemoverChannel <- data
}

// collect sends a group of results to the writer for writing out to the database
func (w *writer) collectUpdater(data update) {
	w.updaterChannel <- data
}

// closeCIDRemover waits for the write threads to finish
func (w *writer) closeCIDRemover() {
	close(w.cidRemoverChannel)
	w.writeWg.Wait()
}

// closeUpdater waits for the write threads to finish
func (w *writer) closeUpdater() {
	close(w.updaterChannel)
	w.writeWg.Wait()
}

// startCIDRemover kicks off a new write thread
func (w *writer) startCIDRemover() {
	w.writeWg.Add(1)
	go func() {
		for data := range w.cidRemoverChannel {
			//delete the ENTIRE record if it hasn't been updated since the chunk we are trying to remove
			delInfo, err := w.db.Client.Database(w.db.GetSelectedDB()).Collection(data).DeleteMany(w.db.Context, bson.M{"cid": w.cid})
			if err != nil || delInfo.DeletedCount == 0 {
				w.log.WithFields(log.Fields{
					"Module":  "remover",
					"Info":    delInfo,
					"Data":    data,
					"Message": "failed to delete whole document",
				}).Error(err)
			}

			// this ONLY deletes a specific chunk's DATA from a record that HAS been updated recently and doesn't need to be completely
			// removed - only the target chunk's stats should be removed from it
			updateInfo, err := w.db.Client.Database(w.db.GetSelectedDB()).Collection(data).UpdateMany(w.db.Context, bson.M{"dat.cid": w.cid}, bson.M{"$pull": bson.M{"dat": bson.M{"cid": w.cid}}})
			if err != nil ||
				((updateInfo.ModifiedCount == 0) && (updateInfo.UpsertedCount == 0) && (updateInfo.MatchedCount != 0)) {
				w.log.WithFields(log.Fields{
					"Module":  "remover",
					"Info":    updateInfo,
					"Data":    data,
					"Message": "failed to delete chunk",
				}).Error(err)
			}

		}
		w.writeWg.Done()
	}()
}

// startUpdater kicks off a new write thread
func (w *writer) startUpdater() {
	w.writeWg.Add(1)
	go func() {
		for data := range w.updaterChannel {
			_, err := w.db.Client.Database(w.db.GetSelectedDB()).Collection(data.collection).UpdateMany(w.db.Context, data.selector, data.query)
			if err != nil {
				fmt.Println(err, data)
			}

		}
		w.writeWg.Done()
	}()
}
