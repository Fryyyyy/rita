package hostname

import (
	"github.com/activecm/rita/pkg/data"
	"github.com/activecm/rita/resources"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// IPResults returns the IP addresses the hostname was seen resolving to in the dataset
func IPResults(res *resources.Resources, hostname string) ([]data.UniqueIP, error) {

	ipsForHostnameQuery := []bson.M{
		{"$match": bson.M{
			"host": hostname,
		}},
		{"$project": bson.M{
			"ips": "$dat.ips",
		}},
		{"$unwind": "$ips"},
		{"$unwind": "$ips"},
		{"$group": bson.M{
			"_id": bson.M{
				"ip":           "$ips.ip",
				"network_uuid": "$ips.network_uuid",
			},
			"network_name": bson.M{"$last": "$ips.network_name"},
		}},
		{"$project": bson.M{
			"_id":          0,
			"ip":           "$_id.ip",
			"network_uuid": "$_id.network_uuid",
			"network_name": "$network_name",
		}},
		{"$sort": bson.M{
			"ip": 1,
		}},
	}

	var ipResults []data.UniqueIP
	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := res.DB.Client.Database(res.DB.GetSelectedDB()).Collection(res.Config.T.DNS.HostnamesTable).Aggregate(res.DB.Context, ipsForHostnameQuery, opts)
	if err != nil {
		return ipResults, err
	}
	err = cursor.All(res.DB.Context, &ipResults)
	return ipResults, err
}

// FQDNResults returns the FQDNs the IP address was seen resolving to in the dataset
func FQDNResults(res *resources.Resources, hostIP string) ([]*FQDNResult, error) {
	fqdnsForHostnameQuery := []bson.M{
		{"$match": bson.M{
			"dat.ips.ip": hostIP,
		}},
		{"$group": bson.M{
			"_id": "$host",
		}},
	}

	var fqdnResults []*FQDNResult
	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := res.DB.Client.Database(res.DB.GetSelectedDB()).Collection(res.Config.T.DNS.HostnamesTable).Aggregate(res.DB.Context, fqdnsForHostnameQuery, opts)
	if err != nil {
		return fqdnResults, err
	}
	err = cursor.All(res.DB.Context, &fqdnResults)
	return fqdnResults, err
}
