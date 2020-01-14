package gousupostgres

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/indece-official/go-gousu"
	"github.com/namsral/flag"

	"github.com/lib/pq"
	// Use postgres driver for database/sql
	_ "github.com/lib/pq"
)

// ServiceName defines the name of postgres service used for dependency injection
const ServiceName = "postgres"

var (
	postgresHost          = flag.String("postgres_host", "localhost", "")
	postgresPort          = flag.Int("postgres_port", 5432, "")
	postgresUser          = flag.String("postgres_user", "", "")
	postgresPassword      = flag.String("postgres_password", "", "")
	postgresDatabase      = flag.String("postgres_database", "", "")
	postgresMaxRetries    = flag.Int("postgres_max_retries", 10, "")
	postgresRetryInterval = flag.Int("postgres_retry_interval", 6, "")
)

// Options can contain parameters passed to the postgres service
type Options struct {
	// SetupSQL can contain the content of a sql-file for updating the
	// database on startup
	SetupSQL string

	// UpdateSQL can contain the content of a sql-file for updating the
	// database on startup
	UpdateSQL string

	// GetDBRevisionSQL can be used for retrieving the revision of the database
	// used, must return/select one integer field
	GetDBRevisionSQL string
}

// IService defined the interface of the postgresql database service
type IService interface {
	gousu.IService

	GetDB() *sql.DB
}

// Service provides the interaction with the postgresql database
type Service struct {
	error   error
	log     *gousu.Log
	db      *sql.DB
	options *Options
}

var _ IService = (*Service)(nil)

func (s *Service) nullTimeToTime(nt *pq.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}

	return &nt.Time
}

// Name returns the name of redis service from ServiceName
func (s *Service) Name() string {
	return ServiceName
}

// GetDB returns the postgres db connection
func (s *Service) GetDB() *sql.DB {
	return s.db
}

// Start initializes the connection to the postgres database and executed both setup.sql and update.sql
// after connecting
func (s *Service) Start() error {
	var err error

	s.error = nil

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", *postgresUser, *postgresPassword, *postgresHost, *postgresPort, *postgresDatabase)

	retries := 0

	for retries < *postgresMaxRetries {
		s.log.Infof("Connecting to postgres database on %s:%d ...", *postgresHost, *postgresPort)

		s.db, s.error = sql.Open("postgres", connStr)
		if s.error == nil {
			s.error = s.db.Ping()
			if s.error == nil {
				s.log.Infof("Connected to postgres database on %s:%d", *postgresHost, *postgresPort)

				break
			}
		}

		s.log.Errorf("Can't connect to postgres on %s:%d: %s", *postgresHost, *postgresPort, s.error)

		time.Sleep(time.Second * time.Duration(*postgresRetryInterval))
		retries++
	}

	if s.error != nil {
		s.log.Errorf("Can't connect to postgres on %s:%d after %d attempts: %s", *postgresHost, *postgresPort, retries, s.error)

		return s.error
	}

	if s.options.SetupSQL != "" {
		s.log.Infof("Executing setup SQL ...")

		_, err = s.db.Exec(s.options.SetupSQL)
		if err != nil {
			s.log.Errorf("Error executing setup SQL: %s", err)

			return err
		}
	}

	if s.options.UpdateSQL != "" {
		s.log.Infof("Executing update SQL ...")

		_, err = s.db.Exec(s.options.UpdateSQL)
		if err != nil {
			s.log.Errorf("Error executing update SQL: %s", err)

			return err
		}
	}

	if s.options.GetDBRevisionSQL != "" {
		var rev int
		err = s.db.QueryRow(s.options.GetDBRevisionSQL).Scan(&rev)
		if err != nil {
			s.log.Errorf("Retrieving revision from database failed: %s", err)

			return err
		}

		s.log.Infof("Using database rev.%d", rev)
	}

	return nil
}

// Stop currently does nothing
func (s *Service) Stop() error {
	return nil
}

// Health checks the health of the postgres-service by pinging the postgres database
func (s *Service) Health() error {
	if s.error != nil {
		return s.error
	}

	return s.db.Ping()
}

// NewServiceBase creates a new instance of postgres-service, should be used instead
//  of generating it manually
func NewServiceBase(ctx gousu.IContext, options *Options) *Service {
	if options == nil {
		options = &Options{}
	}

	return &Service{
		options: options,
		log:     gousu.GetLogger(fmt.Sprintf("service.%s", ServiceName)),
	}
}
