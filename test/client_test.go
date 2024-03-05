package test

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCreateRedisClient(t *testing.T) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	assert.NotNil(t, redisClient)

	err := redisClient.Close()
	assert.Nil(t, err)
}

func TestPingRedis(t *testing.T) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	result, err := redisClient.Ping(context.Background()).Result()
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	t.Run("test set expired success", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			DB:   0,
		})

		ctx := context.Background()
		name := "Reo Sahobby"
		err := client.SetEx(ctx, "name", name, time.Second*3).Err()
		assert.Nil(t, err)

		// get data
		result, err := client.Get(ctx, "name").Result()
		assert.Nil(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, name, result)
	})
	t.Run("test set expired get null data", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			DB:   0,
		})

		ctx := context.Background()
		err := client.SetEx(ctx, "address", "Jakarta Selatan", time.Second*2).Err()
		assert.Nil(t, err)

		time.Sleep(3 * time.Second)
		result, err := client.Get(ctx, "address").Result()
		assert.Equal(t, "", result)
		assert.NotNil(t, err)
		assert.Error(t, err)
	})
}

func TestList(t *testing.T) {
	// create redis client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	ctx := context.Background()
	client.Ping(ctx)
	client.RPush(ctx, "names", "Eko")
	client.RPush(ctx, "names", "Kurniawan")
	client.RPush(ctx, "names", "Khannedy")

	// get data with LPop and RPop
	name, err := client.LPop(ctx, "names").Result()
	assert.Nil(t, err)
	assert.Equal(t, "Eko", name)

	name = client.RPop(ctx, "names").Val()
	assert.Equal(t, "Khannedy", name)

	name = client.RPop(ctx, "names").Val()
	assert.Equal(t, "Kurniawan", name)

	client.Del(ctx, "names")
}

func TestSet(t *testing.T) {
	// create client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	ctx := context.Background()
	keyName := "students"
	client.SAdd(ctx, keyName, "Eko")
	client.SAdd(ctx, keyName, "Eko")
	client.SAdd(ctx, keyName, "Kurniawan")
	client.SAdd(ctx, keyName, "Khannedy")

	result, err := client.SCard(ctx, keyName).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(3), result)

	results, err := client.SMembers(ctx, keyName).Result()
	assert.Nil(t, err)
	assert.Equal(t, []string{"Eko", "Kurniawan", "Khannedy"}, results)

	client.Del(ctx, keyName)
}

func TestSortedSet(t *testing.T) {
	// create client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	ctx := context.Background()
	keyName := "scores"
	err := client.ZAdd(ctx, keyName, redis.Z{
		Score:  100,
		Member: "Eko",
	}).Err()
	assert.Nil(t, err)

	err = client.ZAdd(ctx, keyName, redis.Z{
		Score:  85,
		Member: "Budi",
	}).Err()
	assert.Nil(t, err)

	err = client.ZAdd(ctx, keyName, redis.Z{
		Score:  95,
		Member: "Joko",
	}).Err()
	assert.Nil(t, err)

	// get data
	results := client.ZRange(ctx, keyName, 0, -1).Val()
	assert.Equal(t, []string{"Budi", "Joko", "Eko"}, results)
	assert.Equal(t, "Eko", client.ZPopMax(ctx, keyName).Val()[0].Member)
	assert.Equal(t, "Joko", client.ZPopMax(ctx, keyName).Val()[0].Member)
	assert.Equal(t, "Budi", client.ZPopMax(ctx, keyName).Val()[0].Member)
}

func TestHash(t *testing.T) {
	// create client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	ctx := context.Background()
	keyName := "user.1"

	// insert set
	client.HSet(ctx, keyName, map[string]string{
		"id":    "1",
		"name":  "Eko",
		"email": "eko@example.com",
	})

	// get data
	result, err := client.HGetAll(ctx, keyName).Result()
	assert.Nil(t, err)

	assert.Equal(t, "1", result["id"])
	assert.Equal(t, "Eko", result["name"])
	assert.Equal(t, "eko@example.com", result["email"])

	client.Del(ctx, keyName)
}

func TestGeoPoint(t *testing.T) {
	// create redis client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	// menambah data geopoint
	ctx := context.Background()
	keyName := "sellers"
	err := client.GeoAdd(ctx, keyName, &redis.GeoLocation{
		Name:      "Toko A",
		Longitude: 106.822702,
		Latitude:  -6.177590,
	}).Err()
	assert.Nil(t, err)

	err = client.GeoAdd(ctx, keyName, &redis.GeoLocation{
		Name:      "Toko B",
		Longitude: 106.820889,
		Latitude:  -6.174964,
	}).Err()
	assert.Nil(t, err)

	distance, err := client.GeoDist(ctx, keyName, "Toko A", "Toko B", "KM").Result()
	assert.Nil(t, err)
	assert.NotNil(t, distance)
	assert.Equal(t, 0.3543, distance)

	radiusResult, err := client.GeoSearch(ctx, keyName, &redis.GeoSearchQuery{
		Longitude:  106.819143,
		Latitude:   -6.180182,
		Radius:     5,
		RadiusUnit: "KM",
	}).Result()
	assert.Nil(t, err)
	assert.NotNil(t, radiusResult)
	assert.Equal(t, []string{"Toko A", "Toko B"}, radiusResult)
}

func TestHyperLogLog(t *testing.T) {
	// create client redis
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer client.Close()

	ctx := context.Background()
	keyName := "visitors"

	// add data to hyperloglog
	err := client.PFAdd(ctx, keyName, "eko", "kurniawan", "khannedy").Err()
	assert.Nil(t, err)

	err = client.PFAdd(ctx, keyName, "eko", "budi", "joko").Err()
	assert.Nil(t, err)

	err = client.PFAdd(ctx, keyName, "budi", "joko", "rully").Err()
	assert.Nil(t, err)

	result, err := client.PFCount(ctx, keyName).Result()
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(6), result)
}

func TestPipelineRedis(t *testing.T) {
	// create redis client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// create pipeline
	ctx := context.Background()
	client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "Reo", time.Second*3)
		pipeliner.SetEx(ctx, "address", "Jakarta Selatan", time.Second*3)
		return nil
	})

	// test
	assert.Equal(t, "Reo", client.Get(ctx, "name").Val())
	assert.Equal(t, "Jakarta Selatan", client.Get(ctx, "address").Val())
}
