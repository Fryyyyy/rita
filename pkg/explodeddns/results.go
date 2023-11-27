package explodeddns

import (
	"github.com/activecm/rita/resources"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Results returns hostnames and their subdomain/ lookup statistics from the database.
// limit and noLimit control how many results are returned.
func Results(res *resources.Resources, limit int, noLimit bool) ([]Result, error) {

	var explodedDNSResults []Result

	explodedDNSQuery := []bson.M{
		bson.M{"$unwind": "$dat"},
		bson.M{"$project": bson.M{"domain": 1, "subdomain_count": 1, "visited": "$dat.visited"}},
		bson.M{"$group": bson.M{
			"_id":             "$domain",
			"visited":         bson.M{"$sum": "$visited"},
			"subdomain_count": bson.M{"$first": "$subdomain_count"},
		}},
		bson.M{"$project": bson.M{
			"_id":             0,
			"domain":          "$_id",
			"visited":         1,
			"subdomain_count": 1,
		}},
		bson.M{"$sort": bson.M{"visited": -1}},
		bson.M{"$sort": bson.M{"subdomain_count": -1}},
	}

	if !noLimit {
		explodedDNSQuery = append(explodedDNSQuery, bson.M{"$limit": limit})
	}

	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := res.DB.Client.Database(res.DB.GetSelectedDB()).Collection(res.Config.T.DNS.ExplodedDNSTable).Aggregate(res.DB.Context, explodedDNSQuery, opts)
	if err != nil {
		return explodedDNSResults, err
	}
	cursor.All(res.DB.Context, &explodedDNSResults)
	return explodedDNSResults, err

}
