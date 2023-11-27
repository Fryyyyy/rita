package beaconproxy

import (
	"github.com/activecm/rita/resources"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Results finds beacons FQDN in the database greater than a given cutoffScore
func Results(res *resources.Resources, cutoffScore float64) ([]Result, error) {
	var beaconsProxy []Result

	BeaconProxyQuery := bson.M{"score": bson.M{"$gt": cutoffScore}}

	cursor, err := res.DB.Client.Database(res.DB.GetSelectedDB()).Collection(res.Config.T.BeaconProxy.BeaconProxyTable).Find(res.DB.Context, BeaconProxyQuery)
	if err != nil {
		return beaconsProxy, err
	}
	options.Find().SetSort(bson.D{{"score", -1}})
	err = cursor.All(res.DB.Context, &beaconsProxy)
	return beaconsProxy, err
}
