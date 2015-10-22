package mds

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="utf-8"?>
<post obj="sandbox-tmp.file1" id="0:48f22774edb9...7727258a3cee" groups="2" size="4" key="3402/file1">
<complete addr="192.168.1.1:1025" path="/srv/storage/47/1/data-0.0" group="4643" status="0"/>
<complete addr="192.168.1.2:1025" path="/srv/storage/60/2/data-0.0" group="3402" status="0"/>
<written>2</written>
</post>`)
	var info UploadInfo
	if err := decodeUploadInfo(&info, bytes.NewReader(body)); err != nil {
		t.Fatalf("unable to decode %+v", err)
	}

	assert.Equal(t, uint64(4), info.Size)
	assert.Equal(t, "3402/file1", info.Key)
	assert.Equal(t, "0:48f22774edb9...7727258a3cee", info.ID)
	assert.Equal(t, "sandbox-tmp.file1", info.Obj)
	assert.Equal(t, 2, info.Groups)

	if !assert.Equal(t, 2, len(info.Complete)) {
		t.FailNow()
	}

	compl0 := info.Complete[0]
	assert.Equal(t, "192.168.1.1:1025", compl0.Addr)
	assert.Equal(t, "/srv/storage/47/1/data-0.0", compl0.Path)
	assert.Equal(t, 4643, compl0.Group)
	assert.Equal(t, 0, compl0.Status)

	compl1 := info.Complete[1]
	assert.Equal(t, "192.168.1.2:1025", compl1.Addr)
	assert.Equal(t, "/srv/storage/60/2/data-0.0", compl1.Path)
	assert.Equal(t, 3402, compl1.Group)
	assert.Equal(t, 0, compl1.Status)

	assert.Equal(t, 2, info.Written)
}

func TestUploadAndGet(t *testing.T) {
	const (
		namespace = "sandbox-tmp"
		keyPrefix = "noxiouz"

		rangeStart = 2
		rangeEnd   = 4
	)
	body := []byte("TESTBLOB")

	cli, err := NewClient(Config{
		Host:       "storage-int.mdst.yandex.net",
		UploadPort: 1111,
		ReadPort:   80,
		AuthHeader: "Basic c2FuZGJveC10bXA6YjUyZDVkZjk0ZDA0NTU2MTRiZDZmOWI3NDA3Mzk0OWI=",
	})

	if !assert.NoError(t, err) {
		t.FailNow()
	}
	info, err := cli.Upload(namespace, fmt.Sprintf("%s-%d", keyPrefix, time.Now().Nanosecond()),
		ioutil.NopCloser(bytes.NewReader(body)))

	if !assert.NoError(t, err) {
		t.Fatal("unable to upload")
	}

	cfile, err := cli.GetFile(namespace, info.Key)
	assert.NoError(t, err)
	assert.Equal(t, body, cfile)

	cfile, err = cli.GetFile(namespace, info.Key, rangeStart)
	assert.NoError(t, err)
	assert.Equal(t, body[rangeStart:], cfile)

	cfile, err = cli.GetFile(namespace, info.Key, rangeStart, rangeEnd)
	assert.NoError(t, err)
	assert.Equal(t, body[rangeStart:rangeEnd+1], cfile)
}
