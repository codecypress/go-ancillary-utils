package cypressutils

import (
	"errors"
)

type PageableWrapper struct {
	Domain      string      `json:"domain"`
	CurrentPage int         `json:"current_page"`
	LastPage    int         `json:"last_page"`
	PageSize    int         `json:"page_size"`
	TotalCount  int         `json:"total_count"`
	Data        interface{} `json:"data"`
}

func NewPageableWrapper() *PageableWrapper {
	return &PageableWrapper{}
}

func (wrapper *PageableWrapper) GetDomain() string {
	return wrapper.Domain
}

func (wrapper *PageableWrapper) SetDomain(domain string) {
	wrapper.Domain = domain
}

func (wrapper *PageableWrapper) GetCurrentPage() int {
	return wrapper.CurrentPage
}

func (wrapper *PageableWrapper) SetCurrentPage(currentPage int) {
	wrapper.CurrentPage = currentPage
}

func (wrapper *PageableWrapper) GetLastPage() int {
	return wrapper.LastPage
}

func (wrapper *PageableWrapper) SetLastPage(lastPage int) {
	wrapper.LastPage = lastPage
}

func (wrapper *PageableWrapper) GetPageSize() int {
	return wrapper.PageSize
}

func (wrapper *PageableWrapper) SetPageSize(pageSize int) {
	wrapper.PageSize = pageSize
}

func (wrapper *PageableWrapper) GetTotalCount() int {
	return wrapper.TotalCount
}

func (wrapper *PageableWrapper) SetTotalCount(totalCount int) {
	wrapper.TotalCount = totalCount
}

func (wrapper *PageableWrapper) GetData() interface{} {
	return wrapper.Data
}

func (wrapper *PageableWrapper) SetData(data interface{}) {
	wrapper.Data = data
}

func (wrapper *PageableWrapper) GetSingleRecord() (*CypressHashMap, error) {
	//interface{}.(Data).(type)
	switch wrapper.Data.(type) {
	case *CypressArrayList:
		{
			list := wrapper.Data.(*CypressArrayList)
			return list.GetRecord(0), nil
		}
	case *CypressHashMap:
		{
			return wrapper.Data.(*CypressHashMap), nil
		}
	default:
		return nil, errors.New("failed to call 'getSingleRecord()'. Can only be called if data is of type CypressHashMap, CypressArrayList")
	}
}
