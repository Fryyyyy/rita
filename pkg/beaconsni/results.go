package beaconsni

import (
	"github.com/activecm/rita/resources"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Results finds SNI beacons in the database greater than a given cutoffScore
func Results(res *resources.Resources, cutoffScore float64) ([]Result, error) {
	var beaconsSNI []Result

	beaconSNIQuery := bson.M{"score": bson.M{"$gt": cutoffScore}}

	cursor, err := res.DB.Client.Database(res.DB.GetSelectedDB()).Collection(res.Config.T.BeaconSNI.BeaconSNITable).Find(res.DB.Context, beaconSNIQuery)
	if err != nil {
		return beaconsSNI, err
	}

	options.Find().SetSort(bson.D{{"score", -1}})
	cursor.All(res.DB.Context, &beaconsSNI)
	return beaconsSNI, err
}
