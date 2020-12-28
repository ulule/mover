package etl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetQueryTable(t *testing.T) {
	assert.Equal(t, "ulule_project", getQueryTable("select * from ulule_project"))
	assert.Equal(t, "ulule_project", getQueryTable("SELECT * FROM ulule_project"))
	assert.Equal(t, "ulule_project", getQueryTable("SELECT one, two, three FROM ulule_project"))
}
