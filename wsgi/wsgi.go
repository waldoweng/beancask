package wsgi

import (
	"net/http"

	"github.com/emicklei/go-restful"
	restfulspec "github.com/emicklei/go-restful-openapi"
	"github.com/waldoweng/beancask/storage"
)

// Wsgi wsgi-like struct of the restful service
type Wsgi struct {
	Bitcask *storage.Bitcask
}

// Data struct for input and output data
type Data struct {
	Key   string `json:"key" description:"key of the data"`
	Value string `json:"value" description:"value of the data" default:""`
}

func (u Wsgi) webService() *restful.WebService {
	ws := new(restful.WebService)
	ws.
		Path("/bitcask").
		Consumes(restful.MIME_XML, restful.MIME_JSON).
		Produces(restful.MIME_JSON, restful.MIME_XML)

	tags := []string{"bitcask"}

	ws.Route(ws.GET("/{key}").To(u.get).
		Doc("get a value by key").
		Param(ws.PathParameter("key", "key of the data").DataType("string")).
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(Data{}).
		Returns(200, "OK", Data{}).
		Returns(404, "NOT FOUND", nil))

	ws.Route(ws.PUT("/").To(u.set).
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Reads(Data{}))

	return ws
}

func (u *Wsgi) get(request *restful.Request, response *restful.Response) {
	key := request.PathParameter("key")
	value, err := u.Bitcask.Get(key)
	if err != nil {
		response.WriteErrorString(http.StatusNotFound, err.Error())
	} else {
		response.WriteEntity(Data{key, value})
	}
}

func (u *Wsgi) set(request *restful.Request, response *restful.Response) {
	data := new(Data)
	err := request.ReadEntity(&data)
	if err == nil {
		u.Bitcask.Set(data.Key, data.Value)
		response.WriteEntity(data)
	} else {
		response.WriteError(http.StatusInternalServerError, err)
	}
}

func (u *Wsgi) del(request *restful.Request, response *restful.Response) {
	key := request.PathParameter("key")
	err := u.Bitcask.Delete(key)
	if err != nil {
		response.WriteErrorString(http.StatusNotFound, err.Error())
	} else {
		response.WriteErrorString(http.StatusOK, "ok")
	}
}

// SetUpRestfulService create a restful service and start serving request
func SetUpRestfulService(u Wsgi) {
	restful.DefaultContainer.Add(u.webService())

	cors := restful.CrossOriginResourceSharing{
		AllowedHeaders: []string{"Content-Type", "Accept"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE"},
		CookiesAllowed: false,
		Container:      restful.DefaultContainer,
	}
	restful.DefaultContainer.Filter(cors.Filter)
}
