package etl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ulule/mover/config"
)

func TestSanitizeValues(t *testing.T) {
	var (
		emailReplace = "ulule-{id}@ulule.com"
		nameReplace  = "{username}"
		userSchema   = config.Schema{
			TableName: "user",
			Columns: []config.Column{
				{
					Name:    "name",
					Replace: &nameReplace,
				},
				{
					Name:    "email",
					Replace: &emailReplace,
				},
				{
					Name:     "password",
					Sanitize: true,
				},
			},
		}
	)

	sanitizer := newSanitizer("fr", map[string]config.Schema{
		"user": userSchema,
	})

	results := sanitizer.sanitizeValues(userSchema, map[string]interface{}{
		"username": "thoas",
		"name":     "Florent Messa",
		"email":    "florent@ulule.com",
		"password": "$ecret",
		"id":       1,
	})
	assert.Equal(t, "ulule-1@ulule.com", results["email"])
	assert.Equal(t, nil, results["password"])
	assert.Equal(t, "thoas", results["name"])
}
