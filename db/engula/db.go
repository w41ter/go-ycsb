package engula

import (
	"context"
	"fmt"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	engula_go "github.com/w41ter/engula-go/pkg"
)

const (
	engulaProxy      = "engula.proxy"
	engulaDatabase   = "engula.db"
	engulaCollection = "engula.collection"
	engulaConn       = "engula.conn"
)

type engulaCreator struct{}

func (c *engulaCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	addrs := p.GetString(engulaProxy, "127.0.0.1:21805")
	conn := p.GetInt(engulaConn, 8)
	resolverBuilder := engula_go.NewStaticResolverBuilder(uint16(conn), strings.Split(addrs, ","))
	cfg := engula_go.DefaultConfig()
	cfg.ResolverBuilder = resolverBuilder
	cfg.Endpoints = resolverBuilder.Endpoints()
	client, err := engula_go.New(cfg)
	if err != nil {
		return nil, err
	}

	dbName := p.GetString(engulaDatabase, "db")
	coName := p.GetString(engulaCollection, "co")
	db, err := client.GetDatabase(context.Background(), dbName)
	if err == engula_go.ErrNotFound {
		db, err = client.CreateDatabase(context.Background(), dbName)
	}
	if err != nil {
		return nil, err
	}

	collection, err := db.GetCollection(context.Background(), coName)
	if err == engula_go.ErrNotFound {
		collection, err = db.CreateHashCollection(context.Background(), coName, 64)
	}
	if err != nil {
		return nil, err
	}

	bufPool := util.NewBufPool()

	return &engulaDB{
		client:  client,
		co:      collection,
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: bufPool,
	}, nil
}

func init() {
	ycsb.RegisterDBCreator("engula", &engulaCreator{})
}

type engulaDB struct {
	client  *engula_go.Client
	db      *engula_go.Database
	co      *engula_go.Collection
	r       *util.RowCodec
	bufPool *util.BufPool
}

func (db *engulaDB) Close() error {
	db.client.Close()
	return nil
}

func (db *engulaDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *engulaDB) CleanupThread(ctx context.Context) {
}

func (db *engulaDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *engulaDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	row, err := db.co.Get(ctx, db.getRowKey(table, key))
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	return db.r.Decode(row, fields)
}

func (db *engulaDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	panic("not implemented")
}

func (db *engulaDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	panic("not implemented")
}

func (db *engulaDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	row, err := db.co.Get(ctx, db.getRowKey(table, key))
	if err != nil {
		return nil
	}

	data, err := db.r.Decode(row, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		data[field] = value
	}

	// Update data and use Insert to overwrite.
	return db.Insert(ctx, table, key, data)
}

func (db *engulaDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("not implemented")
}

func (db *engulaDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	return db.co.Put(ctx, db.getRowKey(table, key), buf)
}

func (db *engulaDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("not implemented")
}

func (db *engulaDB) Delete(ctx context.Context, table string, key string) error {
	return db.co.Delete(ctx, db.getRowKey(table, key))
}

func (db *engulaDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	panic("not implemented")
}
