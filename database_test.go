package dbutils

import (
	"testing"
	"time"
)

func TestConn(t *testing.T) {
	data := Database{}.GetConn().ExecSql("select count(*) from rc_config")
	print(data)
}

func TestInsert(t *testing.T) {
	meeting := make(map[string]interface{})
	mockMeeting(meeting, "22222")
	meetingTableId := Database{}.GetConn().Insert("rc_meeting", meeting)
	println(meetingTableId)
}

func mockMeeting(meeting map[string]interface{}, extensionId string) {
	meeting["host"] = "test"
	meeting["meetingid"] = extensionId
	meeting["meetingtype"] = 0
	meeting["ownerid"] = extensionId
	meeting["recordingamount"] = 1
	meeting["starttime"] = time.Now()
	meeting["topic"] = "topic" + extensionId
	meeting["updatetime"] = time.Now()
	meeting["autoretrycount"] = 0
	meeting["manualretrycount"] = 0
	meeting["meetingstatus"] = 1
}
