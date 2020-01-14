package gousupostgres

import (
	"database/sql"

	"github.com/indece-official/go-gousu"
)

// MockService for simply mocking IService
type MockService struct {
	gousu.MockService

	GetDBFunc       func() *sql.DB
	GetDBFuncCalled int
}

// MockService implements IService
var _ (IService) = (*MockService)(nil)

// GetDB calls GetDBFunc and increases GetDBFuncCalled
func (s *MockService) GetDB() *sql.DB {
	s.GetDBFuncCalled++

	return s.GetDBFunc()
}

// NewMockService creates a new initialized instance of MockService
func NewMockService() *MockService {
	return &MockService{
		MockService: gousu.MockService{
			NameFunc: func() string {
				return ServiceName
			},
		},

		GetDBFunc: func() *sql.DB {
			return &sql.DB{}
		},
		GetDBFuncCalled: 0,
	}
}
