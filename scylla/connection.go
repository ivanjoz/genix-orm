package scylla

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

var mu sync.Mutex
var scyllaSession *gocql.Session
var connParams ConnParams = ConnParams{}

// The session is created lazily on the first query, so a node that is still
// booting (Scylla needs tens of seconds to accept CQL after a restart) would
// fail every request until traffic happens to arrive at a lucky moment.
// Retrying with backoff absorbs that window inside a single request.
const connectAttempts = 4
const connectFirstRetryDelay = 500 * time.Millisecond
const connectMaxRetryDelay = 2 * time.Second

// scyllaLogger forwards gocql's own diagnostics into the app log. gocql reports
// the useful part -- per host dial failures, control connection errors, ring
// refresh problems -- only through this logger; the error it returns to the
// caller is the generic "no connections were made", which hides whether the
// dial was refused, timed out, or rejected by authentication.
type scyllaLogger struct{}

func (scyllaLogger) Print(values ...any) { log.Print(append([]any{"ScyllaDB:: "}, values...)...) }
func (scyllaLogger) Printf(format string, values ...any) {
	log.Printf("ScyllaDB:: "+format, values...)
}
func (scyllaLogger) Println(values ...any) {
	log.Println(append([]any{"ScyllaDB::"}, values...)...)
}

type ConnParams struct {
	Host         string
	Port         int
	User         string
	Password     string
	Reconnect    bool
	ConnTimeout  int64 //Seconds
	QueryTimeout int64 //Seconds
	WriteTimeout int64 //Seconds
	Keyspace     string
	// MaxClusteringKey mirrors the node's max_clustering_key_restrictions_per_query.
	// The ORM splits a wider IN fanout into that many values per query instead of
	// letting the server reject the statement. 0 falls back to the MAX_CLUSTERING_KEY
	// environment variable, then to 100 (Scylla's own default).
	MaxClusteringKey int
}

func SetScyllaConnection(params ConnParams) {
	if params.ConnTimeout == 0 {
		params.ConnTimeout = 5
	}
	if params.QueryTimeout == 0 {
		params.QueryTimeout = 15
	}
	if params.WriteTimeout == 0 {
		params.WriteTimeout = params.QueryTimeout
	}
	connParams = params
}

func MakeScyllaConnection(params ConnParams) *gocql.Session {
	if params.ConnTimeout == 0 {
		params.ConnTimeout = 10
	}
	if params.QueryTimeout == 0 {
		params.QueryTimeout = 30
	}
	if params.WriteTimeout == 0 {
		params.WriteTimeout = params.QueryTimeout
	}
	connParams = params
	return getScyllaConnection()
}

func getScyllaConnection() *gocql.Session {
	// The mutex is the only gate: the first caller dials while the rest block,
	// and they find the session already built on the double check below. The
	// previous version spun on a nil session without dialing, so one failed
	// attempt left every concurrent request busy waiting, unable to recover.
	mu.Lock()
	defer mu.Unlock()

	if connParams.Reconnect {
		if scyllaSession != nil {
			scyllaSession.Close()
			scyllaSession = nil
		}
	} else if scyllaSession != nil {
		return scyllaSession
	}

	session, err := dialScyllaWithRetries()
	if err != nil {
		// scyllaSession stays nil on purpose: the next request retries the whole
		// dial instead of inheriting a session that was never usable.
		panic(fmt.Sprintf("gocql: unable to create session after %d attempts: %v", connectAttempts, err))
	}

	fmt.Println("Base de datos ScyllaDB Conectada!!")
	scyllaSession = session
	return scyllaSession
}

// dialScyllaWithRetries retries CreateSession with exponential backoff and
// returns the last error once the attempts are spent. The cluster config is
// rebuilt on every attempt because the token aware host selection policy it
// carries cannot be shared between sessions.
func dialScyllaWithRetries() (*gocql.Session, error) {
	retryDelay := connectFirstRetryDelay

	var lastErr error
	for attempt := 1; attempt <= connectAttempts; attempt++ {
		session, err := buildScyllaCluster().CreateSession()
		if err == nil {
			return session, nil
		}
		lastErr = err
		log.Printf("ScyllaDB:: intento de conexión %d/%d a %v:%v falló: %v",
			attempt, connectAttempts, connParams.Host, connParams.Port, err)

		if attempt < connectAttempts {
			time.Sleep(retryDelay)
			if retryDelay *= 2; retryDelay > connectMaxRetryDelay {
				retryDelay = connectMaxRetryDelay
			}
		}
	}
	return nil, lastErr
}

func buildScyllaCluster() *gocql.ClusterConfig {
	cluster := gocql.NewCluster(connParams.Host)
	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	cluster.Port = connParams.Port
	cluster.Consistency = gocql.LocalOne
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * time.Duration(connParams.ConnTimeout)
	cluster.Timeout = time.Second * time.Duration(connParams.QueryTimeout)
	cluster.WriteTimeout = time.Second * time.Duration(connParams.WriteTimeout)
	cluster.Compressor = gocql.SnappyCompressor{}
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username:              connParams.User,
		Password:              connParams.Password,
		AllowedAuthenticators: []string{"org.apache.cassandra.auth.PasswordAuthenticator"},
	}

	cluster.Logger = scyllaLogger{}

	// The connection pool dials the address each node advertises in system.local
	// / system.peers, not the configured host. A node behind NAT or announcing a
	// private rpc_address makes the control connection succeed and then leaves
	// the pool empty, which surfaces as the generic "no connections were made".
	// Sticking to the configured host removes that failure mode. The cost is
	// that dc, rack and token metadata stay unknown, so TokenAwareHostPolicy
	// degrades to its round robin fallback -- irrelevant while the deployment is
	// a single node; remove this line to regain token aware routing on a real
	// multi node cluster.
	cluster.DisableInitialHostLookup = true

	return cluster
}

func QueryExec(queryStr string, values ...any) error {

	query := getScyllaConnection().Query(queryStr, values...)

	if err := query.Exec(); err != nil {
		if strings.Contains(err.Error(), "no hosts available") {
			fmt.Println(`Error en conexión db: "no hosts available", reconectando...`)
			getScyllaConnection()
			fmt.Println(`Ejecutando query luego de reconexión...`)
			err = query.Exec()
		}
		if err != nil {
			return err
		}
	}
	return nil
}
