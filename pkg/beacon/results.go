package beacon

import (
	"github.com/activecm/rita/resources"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Results finds beacons in the database greater than a given cutoffScore
func Results(res *resources.Resources, cutoffScore float64) ([]Result, error) {
	var beacons []Result

	beaconQuery := bson.M{"score": bson.M{"$gt": cutoffScore}}

	cursor, err := res.DB.Client.Database(res.DB.GetSelectedDB()).Collection(res.Config.T.Beacon.BeaconTable).Find(res.DB.Context, beaconQuery)
	//.Sort("-score").All(&beacons)

	if err != nil {
		return beacons, err
	}
	options.Find().SetSort(bson.D{{Key: "score", Value: -1}})
	err = cursor.All(res.DB.Context, &beacons)
	return beacons, err
}

// StrobeResults finds strobes (beacons with an immense number of connections) in the database.
// The results will be sorted by connection count ordered by sortDir (-1 or 1).
// limit and noLimit control how many results are returned.
func StrobeResults(res *resources.Resources, sortDir, limit int, noLimit bool) ([]StrobeResult, error) {
	var strobes []StrobeResult

	strobeQuery := []bson.M{
		{"$match": bson.M{"strobe": true}},
		{"$unwind": "$dat"},
		{"$project": bson.M{
			"src":              1,
			"src_network_uuid": 1,
			"src_network_name": 1,
			"dst":              1,
			"dst_network_uuid": 1,
			"dst_network_name": 1,
			"conns":            "$dat.count",
		}},
		{"$group": bson.M{
			"_id":              "$_id",
			"src":              bson.M{"$first": "$src"},
			"src_network_uuid": bson.M{"$first": "$src_network_uuid"},
			"src_network_name": bson.M{"$first": "$src_network_name"},
			"dst":              bson.M{"$first": "$dst"},
			"dst_network_uuid": bson.M{"$first": "$dst_network_uuid"},
			"dst_network_name": bson.M{"$first": "$dst_network_name"},
			"connection_count": bson.M{"$sum": "$conns"},
		}},
		{"$sort": bson.M{"connection_count": sortDir}},
	}

	if !noLimit {
		strobeQuery = append(strobeQuery, bson.M{"$limit": limit})
	}

	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := res.DB.Client.Database(res.DB.GetSelectedDB()).Collection(res.Config.T.Structure.UniqueConnTable).Aggregate(res.DB.Context, strobeQuery, opts)
	if err != nil {
		return strobes, err
	}

	err = cursor.All(res.DB.Context, &strobes)

	return strobes, err

}
