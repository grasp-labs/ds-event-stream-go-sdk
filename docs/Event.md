# Event

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **uuid.UUID** | Unique identifier for the event. | 
**SessionId** | **uuid.UUID** | Correlation/session identifier. | 
**RequestId** | **uuid.UUID** | Upstream request identifier. | 
**TenantId** | **uuid.UUID** | Tenant identifier. | 
**OwnerId** | Pointer to [**uuid.UUID**](uuid.UUID.md) | Optional owner id. | [optional] 
**EventType** | [**uuid.UUID**](uuid.UUID.md) | Event type. | 
**EventSource** | [**uuid.UUID**](uuid.UUID.md) | Logical source of the event. | 
**EventSourceUri** | Pointer to **uuid.UUID** | Optional URI describing the event source. | [optional] 
**AffectedEntityUri** | Pointer to **uuid.UUID** | Optional URI of the affected entity. | [optional] 
**Message** | Pointer to [**uuid.UUID**](uuid.UUID.md) | Optional human-readable message. | [optional] 
**Payload** | Pointer to **map[string]interface{}** | JSON body of the event (arbitrary key/value map).  | [optional] 
**PayloadUri** | Pointer to **uuid.UUID** | Optional URI to an external payload. | [optional] 
**Metadata** | [**map[string]uuid.UUID**](uuid.UUID.md) | Arbitrary string key/value metadata. | 
**Tags** | Pointer to [**map[string]uuid.UUID**](uuid.UUID.md) | Optional string key/value tags. | [optional] 
**Timestamp** | **time.Time** | Event timestamp. | 
**CreatedBy** | [**uuid.UUID**](uuid.UUID.md) | Identifier of the creator/producer. | 
**Md5Hash** | [**uuid.UUID**](uuid.UUID.md) | MD5 hash of the canonical event representation. | 
**Context** | Pointer to **map[string]interface{}** | Processing context for consumers (arbitrary key/value map).  | [optional] 
**ContextUri** | Pointer to **uuid.UUID** | Optional URI to external context. | [optional] 

## Methods

### NewEvent

`func NewEvent(id uuid.UUID, sessionId uuid.UUID, requestId uuid.UUID, tenantId uuid.UUID, eventType uuid.UUID, eventSource uuid.UUID, metadata map[string]uuid.UUID, timestamp time.Time, createdBy uuid.UUID, md5Hash uuid.UUID, ) *Event`

NewEvent instantiates a new Event object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewEventWithDefaults

`func NewEventWithDefaults() *Event`

NewEventWithDefaults instantiates a new Event object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *Event) GetId() uuid.UUID`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *Event) GetIdOk() (*uuid.UUID, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *Event) SetId(v uuid.UUID)`

SetId sets Id field to given value.


### GetSessionId

`func (o *Event) GetSessionId() uuid.UUID`

GetSessionId returns the SessionId field if non-nil, zero value otherwise.

### GetSessionIdOk

`func (o *Event) GetSessionIdOk() (*uuid.UUID, bool)`

GetSessionIdOk returns a tuple with the SessionId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSessionId

`func (o *Event) SetSessionId(v uuid.UUID)`

SetSessionId sets SessionId field to given value.


### GetRequestId

`func (o *Event) GetRequestId() uuid.UUID`

GetRequestId returns the RequestId field if non-nil, zero value otherwise.

### GetRequestIdOk

`func (o *Event) GetRequestIdOk() (*uuid.UUID, bool)`

GetRequestIdOk returns a tuple with the RequestId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRequestId

`func (o *Event) SetRequestId(v uuid.UUID)`

SetRequestId sets RequestId field to given value.


### GetTenantId

`func (o *Event) GetTenantId() uuid.UUID`

GetTenantId returns the TenantId field if non-nil, zero value otherwise.

### GetTenantIdOk

`func (o *Event) GetTenantIdOk() (*uuid.UUID, bool)`

GetTenantIdOk returns a tuple with the TenantId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTenantId

`func (o *Event) SetTenantId(v uuid.UUID)`

SetTenantId sets TenantId field to given value.


### GetOwnerId

`func (o *Event) GetOwnerId() uuid.UUID`

GetOwnerId returns the OwnerId field if non-nil, zero value otherwise.

### GetOwnerIdOk

`func (o *Event) GetOwnerIdOk() (*uuid.UUID, bool)`

GetOwnerIdOk returns a tuple with the OwnerId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOwnerId

`func (o *Event) SetOwnerId(v uuid.UUID)`

SetOwnerId sets OwnerId field to given value.

### HasOwnerId

`func (o *Event) HasOwnerId() bool`

HasOwnerId returns a boolean if a field has been set.

### GetEventType

`func (o *Event) GetEventType() uuid.UUID`

GetEventType returns the EventType field if non-nil, zero value otherwise.

### GetEventTypeOk

`func (o *Event) GetEventTypeOk() (*uuid.UUID, bool)`

GetEventTypeOk returns a tuple with the EventType field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEventType

`func (o *Event) SetEventType(v uuid.UUID)`

SetEventType sets EventType field to given value.


### GetEventSource

`func (o *Event) GetEventSource() uuid.UUID`

GetEventSource returns the EventSource field if non-nil, zero value otherwise.

### GetEventSourceOk

`func (o *Event) GetEventSourceOk() (*uuid.UUID, bool)`

GetEventSourceOk returns a tuple with the EventSource field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEventSource

`func (o *Event) SetEventSource(v uuid.UUID)`

SetEventSource sets EventSource field to given value.


### GetEventSourceUri

`func (o *Event) GetEventSourceUri() uuid.UUID`

GetEventSourceUri returns the EventSourceUri field if non-nil, zero value otherwise.

### GetEventSourceUriOk

`func (o *Event) GetEventSourceUriOk() (*uuid.UUID, bool)`

GetEventSourceUriOk returns a tuple with the EventSourceUri field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEventSourceUri

`func (o *Event) SetEventSourceUri(v uuid.UUID)`

SetEventSourceUri sets EventSourceUri field to given value.

### HasEventSourceUri

`func (o *Event) HasEventSourceUri() bool`

HasEventSourceUri returns a boolean if a field has been set.

### GetAffectedEntityUri

`func (o *Event) GetAffectedEntityUri() uuid.UUID`

GetAffectedEntityUri returns the AffectedEntityUri field if non-nil, zero value otherwise.

### GetAffectedEntityUriOk

`func (o *Event) GetAffectedEntityUriOk() (*uuid.UUID, bool)`

GetAffectedEntityUriOk returns a tuple with the AffectedEntityUri field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAffectedEntityUri

`func (o *Event) SetAffectedEntityUri(v uuid.UUID)`

SetAffectedEntityUri sets AffectedEntityUri field to given value.

### HasAffectedEntityUri

`func (o *Event) HasAffectedEntityUri() bool`

HasAffectedEntityUri returns a boolean if a field has been set.

### GetMessage

`func (o *Event) GetMessage() uuid.UUID`

GetMessage returns the Message field if non-nil, zero value otherwise.

### GetMessageOk

`func (o *Event) GetMessageOk() (*uuid.UUID, bool)`

GetMessageOk returns a tuple with the Message field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMessage

`func (o *Event) SetMessage(v uuid.UUID)`

SetMessage sets Message field to given value.

### HasMessage

`func (o *Event) HasMessage() bool`

HasMessage returns a boolean if a field has been set.

### GetPayload

`func (o *Event) GetPayload() map[string]interface{}`

GetPayload returns the Payload field if non-nil, zero value otherwise.

### GetPayloadOk

`func (o *Event) GetPayloadOk() (*map[string]interface{}, bool)`

GetPayloadOk returns a tuple with the Payload field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPayload

`func (o *Event) SetPayload(v map[string]interface{})`

SetPayload sets Payload field to given value.

### HasPayload

`func (o *Event) HasPayload() bool`

HasPayload returns a boolean if a field has been set.

### GetPayloadUri

`func (o *Event) GetPayloadUri() uuid.UUID`

GetPayloadUri returns the PayloadUri field if non-nil, zero value otherwise.

### GetPayloadUriOk

`func (o *Event) GetPayloadUriOk() (*uuid.UUID, bool)`

GetPayloadUriOk returns a tuple with the PayloadUri field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPayloadUri

`func (o *Event) SetPayloadUri(v uuid.UUID)`

SetPayloadUri sets PayloadUri field to given value.

### HasPayloadUri

`func (o *Event) HasPayloadUri() bool`

HasPayloadUri returns a boolean if a field has been set.

### GetMetadata

`func (o *Event) GetMetadata() map[string]uuid.UUID`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Event) GetMetadataOk() (*map[string]uuid.UUID, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Event) SetMetadata(v map[string]uuid.UUID)`

SetMetadata sets Metadata field to given value.


### GetTags

`func (o *Event) GetTags() map[string]uuid.UUID`

GetTags returns the Tags field if non-nil, zero value otherwise.

### GetTagsOk

`func (o *Event) GetTagsOk() (*map[string]uuid.UUID, bool)`

GetTagsOk returns a tuple with the Tags field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTags

`func (o *Event) SetTags(v map[string]uuid.UUID)`

SetTags sets Tags field to given value.

### HasTags

`func (o *Event) HasTags() bool`

HasTags returns a boolean if a field has been set.

### GetTimestamp

`func (o *Event) GetTimestamp() time.Time`

GetTimestamp returns the Timestamp field if non-nil, zero value otherwise.

### GetTimestampOk

`func (o *Event) GetTimestampOk() (*time.Time, bool)`

GetTimestampOk returns a tuple with the Timestamp field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTimestamp

`func (o *Event) SetTimestamp(v time.Time)`

SetTimestamp sets Timestamp field to given value.


### GetCreatedBy

`func (o *Event) GetCreatedBy() uuid.UUID`

GetCreatedBy returns the CreatedBy field if non-nil, zero value otherwise.

### GetCreatedByOk

`func (o *Event) GetCreatedByOk() (*uuid.UUID, bool)`

GetCreatedByOk returns a tuple with the CreatedBy field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedBy

`func (o *Event) SetCreatedBy(v uuid.UUID)`

SetCreatedBy sets CreatedBy field to given value.


### GetMd5Hash

`func (o *Event) GetMd5Hash() uuid.UUID`

GetMd5Hash returns the Md5Hash field if non-nil, zero value otherwise.

### GetMd5HashOk

`func (o *Event) GetMd5HashOk() (*uuid.UUID, bool)`

GetMd5HashOk returns a tuple with the Md5Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMd5Hash

`func (o *Event) SetMd5Hash(v uuid.UUID)`

SetMd5Hash sets Md5Hash field to given value.


### GetContext

`func (o *Event) GetContext() map[string]interface{}`

GetContext returns the Context field if non-nil, zero value otherwise.

### GetContextOk

`func (o *Event) GetContextOk() (*map[string]interface{}, bool)`

GetContextOk returns a tuple with the Context field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContext

`func (o *Event) SetContext(v map[string]interface{})`

SetContext sets Context field to given value.

### HasContext

`func (o *Event) HasContext() bool`

HasContext returns a boolean if a field has been set.

### GetContextUri

`func (o *Event) GetContextUri() uuid.UUID`

GetContextUri returns the ContextUri field if non-nil, zero value otherwise.

### GetContextUriOk

`func (o *Event) GetContextUriOk() (*uuid.UUID, bool)`

GetContextUriOk returns a tuple with the ContextUri field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContextUri

`func (o *Event) SetContextUri(v uuid.UUID)`

SetContextUri sets ContextUri field to given value.

### HasContextUri

`func (o *Event) HasContextUri() bool`

HasContextUri returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


