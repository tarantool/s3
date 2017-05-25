/* A C submodule */
#define _XOPEN_SOURCE 600
#include <tarantool/module.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include "libs3.h"
#include <ctype.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static int showResponsePropertiesG = 0;
static S3Protocol protocolG = S3ProtocolHTTP; /* can be HTTPS */
static S3UriStyle uriStyleG = S3UriStylePath;
static int retriesG = 5;
static int verifyPeerG = 0;
static int useSignatureV4G = 1;
static const char *accessKeyIdG = 0;
static const char *secretAccessKeyG = 0;
static const char *regionNameG = 0;
const char *hostname = 0;
static int statusG = 0;
static char errorDetailsG[4096] = { 0 };
//static int forceG = 0;
//static char putenvBufG[256];


// Option prefixes -----------------------------------------------------------
#define SLEEP_UNITS_PER_SECOND 1
#define MULTIPART_CHUNK_SIZE (15 << 20) // multipart is 15M 
#define LOCATION_PREFIX "location="
#define LOCATION_PREFIX_LEN (sizeof(LOCATION_PREFIX) - 1)
#define CANNED_ACL_PREFIX "cannedAcl="
#define CANNED_ACL_PREFIX_LEN (sizeof(CANNED_ACL_PREFIX) - 1)
#define PREFIX_PREFIX "prefix="
#define PREFIX_PREFIX_LEN (sizeof(PREFIX_PREFIX) - 1)
#define MARKER_PREFIX "marker="
#define MARKER_PREFIX_LEN (sizeof(MARKER_PREFIX) - 1)
#define DELIMITER_PREFIX "delimiter="
#define DELIMITER_PREFIX_LEN (sizeof(DELIMITER_PREFIX) - 1)
#define ENCODING_TYPE_PREFIX "encoding-type="
#define ENCODING_TYPE_PREFIX_LEN (sizeof(ENCODING_TYPE_PREFIX) - 1)
#define MAX_UPLOADS_PREFIX "max-uploads="
#define MAX_UPLOADS_PREFIX_LEN (sizeof(MAX_UPLOADS_PREFIX) - 1)
#define KEY_MARKER_PREFIX "key-marker="
#define KEY_MARKER_PREFIX_LEN (sizeof(KEY_MARKER_PREFIX) - 1)
#define UPLOAD_ID_PREFIX "upload-id="
#define UPLOAD_ID_PREFIX_LEN (sizeof(UPLOAD_ID_PREFIX) - 1)
#define MAX_PARTS_PREFIX "max-parts="
#define MAX_PARTS_PREFIX_LEN (sizeof(MAX_PARTS_PREFIX) - 1)
#define PART_NUMBER_MARKER_PREFIX "part-number-marker="
#define PART_NUMBER_MARKER_PREFIX_LEN (sizeof(PART_NUMBER_MARKER_PREFIX) - 1)
#define UPLOAD_ID_MARKER_PREFIX "upload-id-marker="
#define UPLOAD_ID_MARKER_PREFIX_LEN (sizeof(UPLOAD_ID_MARKER_PREFIX) - 1)
#define MAXKEYS_PREFIX "maxkeys="
#define MAXKEYS_PREFIX_LEN (sizeof(MAXKEYS_PREFIX) - 1)
#define FILENAME_PREFIX "filename="
#define FILENAME_PREFIX_LEN (sizeof(FILENAME_PREFIX) - 1)
#define CONTENT_LENGTH_PREFIX "contentLength="
#define CONTENT_LENGTH_PREFIX_LEN (sizeof(CONTENT_LENGTH_PREFIX) - 1)
#define CACHE_CONTROL_PREFIX "cacheControl="
#define CACHE_CONTROL_PREFIX_LEN (sizeof(CACHE_CONTROL_PREFIX) - 1)
#define CONTENT_TYPE_PREFIX "contentType="
#define CONTENT_TYPE_PREFIX_LEN (sizeof(CONTENT_TYPE_PREFIX) - 1)
#define MD5_PREFIX "md5="
#define MD5_PREFIX_LEN (sizeof(MD5_PREFIX) - 1)
#define CONTENT_DISPOSITION_FILENAME_PREFIX "contentDispositionFilename="
#define CONTENT_DISPOSITION_FILENAME_PREFIX_LEN \
    (sizeof(CONTENT_DISPOSITION_FILENAME_PREFIX) - 1)
#define CONTENT_ENCODING_PREFIX "contentEncoding="
#define CONTENT_ENCODING_PREFIX_LEN (sizeof(CONTENT_ENCODING_PREFIX) - 1)
#define EXPIRES_PREFIX "expires="
#define EXPIRES_PREFIX_LEN (sizeof(EXPIRES_PREFIX) - 1)
#define X_AMZ_META_PREFIX "x-amz-meta-"
#define X_AMZ_META_PREFIX_LEN (sizeof(X_AMZ_META_PREFIX) - 1)
#define USE_SERVER_SIDE_ENCRYPTION_PREFIX "useServerSideEncryption="
#define USE_SERVER_SIDE_ENCRYPTION_PREFIX_LEN \
    (sizeof(USE_SERVER_SIDE_ENCRYPTION_PREFIX) - 1)
#define IF_MODIFIED_SINCE_PREFIX "ifModifiedSince="
#define IF_MODIFIED_SINCE_PREFIX_LEN (sizeof(IF_MODIFIED_SINCE_PREFIX) - 1)
#define IF_NOT_MODIFIED_SINCE_PREFIX "ifNotmodifiedSince="
#define IF_NOT_MODIFIED_SINCE_PREFIX_LEN \
    (sizeof(IF_NOT_MODIFIED_SINCE_PREFIX) - 1)
#define IF_MATCH_PREFIX "ifMatch="
#define IF_MATCH_PREFIX_LEN (sizeof(IF_MATCH_PREFIX) - 1)
#define IF_NOT_MATCH_PREFIX "ifNotMatch="
#define IF_NOT_MATCH_PREFIX_LEN (sizeof(IF_NOT_MATCH_PREFIX) - 1)
#define START_BYTE_PREFIX "startByte="
#define START_BYTE_PREFIX_LEN (sizeof(START_BYTE_PREFIX) - 1)
#define BYTE_COUNT_PREFIX "byteCount="
#define BYTE_COUNT_PREFIX_LEN (sizeof(BYTE_COUNT_PREFIX) - 1)
#define ALL_DETAILS_PREFIX "allDetails="
#define ALL_DETAILS_PREFIX_LEN (sizeof(ALL_DETAILS_PREFIX) - 1)
#define NO_STATUS_PREFIX "noStatus="
#define NO_STATUS_PREFIX_LEN (sizeof(NO_STATUS_PREFIX) - 1)
#define RESOURCE_PREFIX "resource="
#define RESOURCE_PREFIX_LEN (sizeof(RESOURCE_PREFIX) - 1)
#define TARGET_BUCKET_PREFIX "targetBucket="
#define TARGET_BUCKET_PREFIX_LEN (sizeof(TARGET_BUCKET_PREFIX) - 1)
#define TARGET_PREFIX_PREFIX "targetPrefix="
#define TARGET_PREFIX_PREFIX_LEN (sizeof(TARGET_PREFIX_PREFIX) - 1)

static int
should_retry(){
    if (retriesG--) {
        // Sleep before next retry; start out with a 1 second sleep
        static int retrySleepInterval = 1 * SLEEP_UNITS_PER_SECOND;
        sleep(retrySleepInterval);
        // Next sleep 1 second longer
        retrySleepInterval++;
        return 1;
    }
    return 0;
}

static void
responseCompleteCallback(S3Status status, const S3ErrorDetails *error, 
			 void *callbackData){
	(void) callbackData;
	statusG = status;
	// Compose the error details message now, although we might not use it.
	// Can't just save a pointer to [error] since it's not guaranteed to last
	// beyond this callback
	int len = 0;
	if (error && error->message) {
		len += snprintf(
			&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
			"  Message: %s\n", error->message
		);
	}
	if (error && error->resource) {
		len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
			"  Resource: %s\n", error->resource);
	}
	if (error && error->furtherDetails) {
		len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
			"  Further Details: %s\n", error->furtherDetails);
	}
	if (error && error->extraDetailsCount) {
		len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
			"%s", "  Extra Details:\n");
		int i;
		for (i = 0; i < error->extraDetailsCount; i++) {
			len += snprintf(
				&(errorDetailsG[len]), 
				sizeof(errorDetailsG) - len, "    %s: %s\n", 
				error->extraDetails[i].name,
				error->extraDetails[i].value
			);
		}
	}
}


static S3Status
responsePropertiesCallback(const S3ResponseProperties *properties,
			  void *callbackData){
	(void) callbackData;

	if (!showResponsePropertiesG) {
		return S3StatusOK;
	}

	#define print_nonnull(name, field)                                 \
	do {                                                               \
		if (properties-> field) {                                  \
			printf("%s: %s\n", name, properties-> field);      \
		}                                                          \
	} while (0)
    
	print_nonnull("Content-Type", contentType);
	print_nonnull("Request-Id", requestId);
	print_nonnull("Request-Id-2", requestId2);
	if (properties->contentLength > 0) {
		printf(
			"Content-Length: %llu\n",
	       		(unsigned long long) properties->contentLength
		);
	}
	print_nonnull("Server", server);
	print_nonnull("ETag", eTag);
	if (properties->lastModified > 0) {
		char timebuf[256];
		time_t t = (time_t) properties->lastModified;
		// gmtime is not thread-safe but we don't care here.
		strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
		printf("Last-Modified: %s\n", timebuf);
	}
	int i;
	for (i = 0; i < properties->metaDataCount; i++) {
		printf(
			"x-amz-meta-%s: %s\n",
			properties->metaData[i].name,
			properties->metaData[i].value
		);
	}
	if (properties->usesServerSideEncryption) {
		printf("UsesServerSideEncryption: true\n");
	}

	return S3StatusOK;
}


static
int S3_init(){
	S3Status status;
	int flags = verifyPeerG|S3_INIT_ALL;
	if (useSignatureV4G) flags |= S3_INIT_SIGNATURE_V4;
	if ((status = S3_initialize("s3", flags, hostname)) != S3StatusOK) {
		fprintf(
			stderr, "Failed to initialize libs3: %s\n",
			S3_get_status_name(status)
		);
		return -1;
	}
	if (useSignatureV4G && regionNameG) {
		if ((status = S3_set_region_name(regionNameG)) != S3StatusOK) {
			fprintf(stderr, "Failed to set region name to %s: %s\n",regionNameG, S3_get_status_name(status));
			return -1;
		}
	}
	return 0;
}

static S3Status
getObjectDataCallback(int bufferSize, const char *buffer,
                                      void *callbackData){
	FILE *outfile = (FILE *) callbackData;
	size_t wrote = fwrite(buffer, 1, bufferSize, outfile);
	return ((wrote < (size_t) bufferSize) ?
		S3StatusAbortedByCallback : S3StatusOK);
}


static int
get_object(const char *bucketName, const char* key, const char *filename)
{
	int64_t ifModifiedSince = -1, ifNotModifiedSince = -1;
	const char *ifMatch = 0, *ifNotMatch = 0;
	uint64_t startByte = 0, byteCount = 0;

	FILE *outfile = 0;

	if (filename) {
		struct stat buf;
		if (stat(filename, &buf) == -1) {
			outfile = fopen(filename, "w");
		}
		else {
			outfile = fopen(filename, "r+");
		}
        
		if (!outfile) {
			fprintf(stderr, "\nERROR: Failed to open output file %s: ",filename);
			perror(0);
			return 1;
		}
	}
	else if (showResponsePropertiesG) {
		fprintf(stderr, "\nERROR: get -s requires a filename parameter\n");
		return 1;
	}
	else {
		outfile = stdout;
	}

	S3_init();
	S3BucketContext bucketContext =
	{
		0,
		bucketName,
		protocolG,
		uriStyleG,
		accessKeyIdG,
		secretAccessKeyG,
		0
	};

	S3GetConditions getConditions =
	{
		ifModifiedSince,
		ifNotModifiedSince,
		ifMatch,
		ifNotMatch
	};

	S3GetObjectHandler getObjectHandler =
	{
		{ &responsePropertiesCallback, &responseCompleteCallback },
		&getObjectDataCallback
	};

	/* Main get file loop */
	do {
		S3_get_object(
			&bucketContext, key, &getConditions, startByte,
			byteCount, 0, &getObjectHandler, outfile
		);
	} while (S3_status_is_retryable(statusG) && should_retry());

	/* cleanup */
	fclose(outfile);
	S3_deinitialize();
	return 0;
}

typedef struct growbuffer
{
    // The total number of bytes, and the start byte
    int size;
    // The start byte
    int start;
    // The blocks
    char data[64 * 1024];
    struct growbuffer *prev, *next;
} growbuffer;

static void growbuffer_read(growbuffer **gb, int amt, int *amtReturn, 
                            char *buffer)
{
    *amtReturn = 0;
    growbuffer *buf = *gb;
    if (!buf) {
        return;
    }

    *amtReturn = (buf->size > amt) ? amt : buf->size;
    memcpy(buffer, &(buf->data[buf->start]), *amtReturn);
    buf->start += *amtReturn, buf->size -= *amtReturn;

    if (buf->size == 0) {
        if (buf->next == buf) {
            *gb = 0;
        }
        else {
            *gb = buf->next;
            buf->prev->next = buf->next;
            buf->next->prev = buf->prev;
        }
        free(buf);
    }
}

static int growbuffer_append(growbuffer **gb, const char *data, int dataLen)
{
    int toCopy = 0 ;
    while (dataLen) {
        growbuffer *buf = *gb ? (*gb)->prev : 0;
        if (!buf || (buf->size == sizeof(buf->data))) {
            buf = (growbuffer *) malloc(sizeof(growbuffer));
            if (!buf) {
                return 0;
            }
            buf->size = 0;
            buf->start = 0;
            if (*gb && (*gb)->prev) {
                buf->prev = (*gb)->prev;
                buf->next = *gb;
                (*gb)->prev->next = buf;
                (*gb)->prev = buf;
            }
            else {
                buf->prev = buf->next = buf;
                *gb = buf;
            }
        }

        toCopy = (sizeof(buf->data) - buf->size);
        if (toCopy > dataLen) {
            toCopy = dataLen;
        }

        memcpy(&(buf->data[buf->size]), data, toCopy);
        
        buf->size += toCopy, data += toCopy, dataLen -= toCopy;
    }

    return toCopy;
}

static void growbuffer_destroy(growbuffer *gb)
{
    growbuffer *start = gb;
    while (gb) {
        growbuffer *next = gb->next;
        free(gb);
        gb = (next == start) ? 0 : next;
    }
}

typedef struct UploadManager{
    //used for initial multipart
    char * upload_id;

    //used for upload part object
    char **etags;
    int next_etags_pos;

    //used for commit Upload
    growbuffer *gb;
    int remaining;
} UploadManager;

typedef struct put_object_callback_data
{
    FILE *infile;
    growbuffer *gb;
    uint64_t contentLength, originalContentLength;
    uint64_t totalContentLength, totalOriginalContentLength;
    int noStatus;
} put_object_callback_data;

typedef struct MultipartPartData {
    put_object_callback_data put_object_data;
    int seq;
    UploadManager *manager;
} MultipartPartData;

S3Status initial_multipart_callback(const char * upload_id,
                                    void * callbackData)
{
    UploadManager *manager = (UploadManager *) callbackData;
    manager->upload_id = strdup(upload_id);
    return S3StatusOK;
}

S3Status MultipartResponseProperiesCallback
    (const S3ResponseProperties *properties, void *callbackData)
{
    responsePropertiesCallback(properties, callbackData);
    MultipartPartData *data = (MultipartPartData *) callbackData;
    int seq = data->seq;
    const char *etag = properties->eTag;
    data->manager->etags[seq - 1] = strdup(etag);
    data->manager->next_etags_pos = seq;
    return S3StatusOK;
}

static int multipartPutXmlCallback(int bufferSize, char *buffer,
                                   void *callbackData)
{
    UploadManager *manager = (UploadManager*)callbackData;
    int ret = 0;
    if (manager->remaining) {
        int toRead = ((manager->remaining > bufferSize) ?
                      bufferSize : manager->remaining);
        growbuffer_read(&(manager->gb), toRead, &ret, buffer);
    }
    manager->remaining -= ret;
    return ret;
}

typedef struct list_parts_callback_data
{
    int isTruncated;
    char nextPartNumberMarker[24];
    char initiatorId[256];
    char initiatorDisplayName[256];
    char ownerId[256];
    char ownerDisplayName[256];
    char storageClass[256];
    int partsCount;
    int handlePartsStart;
    int allDetails;
    int noPrint;
    UploadManager *manager;
} list_parts_callback_data;

static S3Status listPartsCallback(int isTruncated,
                                        const char *nextPartNumberMarker,
                                        const char *initiatorId,
                                        const char *initiatorDisplayName,
                                        const char *ownerId,
                                        const char *ownerDisplayName,
                                        const char *storageClass,
                                        int partsCount, 
                                        int handlePartsStart,
                                        const S3ListPart *parts,
                                        void *callbackData)
{
    list_parts_callback_data *data = 
        (list_parts_callback_data *) callbackData;

    data->isTruncated = isTruncated;
    data->handlePartsStart = handlePartsStart;
    UploadManager *manager = data->manager;
    /*
    // This is tricky.  S3 doesn't return the NextMarker if there is no
    // delimiter.  Why, I don't know, since it's still useful for paging
    // through results.  We want NextMarker to be the last content in the
    // list, so set it to that if necessary.
    if ((!nextKeyMarker || !nextKeyMarker[0]) && uploadsCount) {
        nextKeyMarker = uploads[uploadsCount - 1].key;
    }*/
    if (nextPartNumberMarker) {
        snprintf(data->nextPartNumberMarker,
                 sizeof(data->nextPartNumberMarker), "%s", 
                 nextPartNumberMarker);
    }
    else {
        data->nextPartNumberMarker[0] = 0;
    }

    if (initiatorId) {
        snprintf(data->initiatorId, sizeof(data->initiatorId), "%s", 
                 initiatorId);
    }
    else {
        data->initiatorId[0] = 0;
    }

    if (initiatorDisplayName) {
        snprintf(data->initiatorDisplayName,
                 sizeof(data->initiatorDisplayName), "%s", 
                 initiatorDisplayName);
    }
    else {
        data->initiatorDisplayName[0] = 0;
    }

    if (ownerId) {
        snprintf(data->ownerId, sizeof(data->ownerId), "%s", 
                 ownerId);
    }
    else {
        data->ownerId[0] = 0;
    }

    if (ownerDisplayName) {
        snprintf(data->ownerDisplayName, sizeof(data->ownerDisplayName), "%s", 
                 ownerDisplayName);
    }
    else {
        data->ownerDisplayName[0] = 0;
    }

    if (storageClass) {
        snprintf(data->storageClass, sizeof(data->storageClass), "%s", 
                 storageClass);
    }
    else {
        data->storageClass[0] = 0;
    }

    int i;
    for (i = 0; i < partsCount; i++) {
        const S3ListPart *part = &(parts[i]);
        char timebuf[256];
        if (data->noPrint) {
            manager->etags[handlePartsStart+i] = strdup(part->eTag);
            manager->next_etags_pos++;
            manager->remaining = manager->remaining - part->size;
        } else {
            time_t t = (time_t) part->lastModified;
            strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ",
                     gmtime(&t));
            printf("%-30s", timebuf);
            printf("%-15llu", (unsigned long long) part->partNumber);
            printf("%-45s", part->eTag);
            printf("%-15llu\n", (unsigned long long) part->size);
        }
    }

    data->partsCount += partsCount;    

    return S3StatusOK;
}

static int try_get_parts_info(const char *bucketName, const char *key, 
                              UploadManager *manager)
{
    S3BucketContext bucketContext =
    {
        0,
        bucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG,
        0
    };

    S3ListPartsHandler listPartsHandler =
    {
        { &responsePropertiesCallback, &responseCompleteCallback },
        &listPartsCallback
    };

    list_parts_callback_data data;

    memset(&data, 0, sizeof(list_parts_callback_data));
   
    data.partsCount = 0;
    data.allDetails = 0;
    data.manager = manager;
    data.noPrint = 1;
    do {
        data.isTruncated = 0;
        do {
            S3_list_parts(&bucketContext, key, data.nextPartNumberMarker,
                          manager->upload_id, 0, 0, 0, &listPartsHandler,
                          &data);
        } while (S3_status_is_retryable(statusG) && should_retry());
        if (statusG != S3StatusOK) {
            break;
        }
    } while (data.isTruncated);

    if (statusG == S3StatusOK) {
        if (!data.partsCount) {
	    fprintf(stderr, "partsCount = 0\n");
	    return 1;
        }
    }
    else {
        return -1;
    }
    
    return 0;
}

static int putObjectDataCallback(int bufferSize, char *buffer,
                                 void *callbackData)
{
    put_object_callback_data *data = 
        (put_object_callback_data *) callbackData;
    
    int ret = 0;

    if (data->contentLength) {
        int toRead = ((data->contentLength > (unsigned) bufferSize) ?
                      (unsigned) bufferSize : data->contentLength);
        if (data->gb) {
            growbuffer_read(&(data->gb), toRead, &ret, buffer);
        }
        else if (data->infile) {
            ret = fread(buffer, 1, toRead, data->infile);
        }
    }

    data->contentLength -= ret;
    data->totalContentLength -= ret;

    return ret;
}

static int put_object(const char *bucketName, const char *key, const char *filename, const char *hash)
{
    const char *uploadId = 0;
    uint64_t contentLength = 0;
    const char *cacheControl = 0, *contentType = 0;
    const char *contentDispositionFilename = 0, *contentEncoding = 0;
    int64_t expires = -1;
    S3CannedAcl cannedAcl = S3CannedAclPrivate;
    int metaPropertiesCount = 0;
    S3NameValue metaProperties[S3_MAX_METADATA_COUNT];
    char useServerSideEncryption = 0;
    int noStatus = 0;

    put_object_callback_data data;

    data.infile = 0;
    data.gb = 0;
    data.noStatus = noStatus;

    if (filename) {
        if (!contentLength) {
            struct stat statbuf;
            // Stat the file to get its length
            if (stat(filename, &statbuf) == -1) {
                fprintf(stderr, "\nERROR: Failed to stat file %s: ",
                        filename);
                perror(0);
		return 1;
            }
            contentLength = statbuf.st_size;
        }
        // Open the file
        if (!(data.infile = fopen(filename, "r"))) {
            fprintf(stderr, "\nERROR: Failed to open input file %s: ",
                    filename);
            perror(0);
	    return 1;
        }
    }

    data.totalContentLength =
    data.totalOriginalContentLength = 
    data.contentLength =
    data.originalContentLength =
            contentLength;

    S3_init();
    S3BucketContext bucketContext =
    {
        0,
        bucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG,
        0
    };

    *strchr(hash, ':') = 0;
    S3PutProperties putProperties =
    {
        contentType,
        hash,
        cacheControl,
        contentDispositionFilename,
        contentEncoding,
        expires,
        cannedAcl,
        metaPropertiesCount,
        metaProperties,
        useServerSideEncryption
    };
    
    if (contentLength <= MULTIPART_CHUNK_SIZE) {
        S3PutObjectHandler putObjectHandler =
        {
            { &responsePropertiesCallback, &responseCompleteCallback },
            &putObjectDataCallback
        };

        do {
            S3_put_object(&bucketContext, key, contentLength, &putProperties, 0,
                          &putObjectHandler, &data);
        } while (S3_status_is_retryable(statusG) && should_retry());

        if (data.infile) {
            fclose(data.infile);
        }
        else if (data.gb) {
            growbuffer_destroy(data.gb);
        }

        if (statusG != S3StatusOK) {
            fprintf(stderr, "Bad status\n");
            return 1;
        }
        else if (data.contentLength) {
            fprintf(stderr, "\nERROR: Failed to read remaining %llu bytes from "
                    "input\n", (unsigned long long) data.contentLength);
        }
    }
    else {
        uint64_t totalContentLength = contentLength;
        uint64_t todoContentLength = contentLength;
        UploadManager manager;
        manager.upload_id = 0;
        manager.gb = 0;

        //div round up
        int seq;
        int totalSeq = ((contentLength + MULTIPART_CHUNK_SIZE- 1) /
                        MULTIPART_CHUNK_SIZE);

        MultipartPartData partData;
        int partContentLength = 0;

        S3MultipartInitialHandler handler = {
            {
                &responsePropertiesCallback,
                &responseCompleteCallback
            },
            &initial_multipart_callback    
        };

        S3PutObjectHandler putObjectHandler = {
            {&MultipartResponseProperiesCallback, &responseCompleteCallback },
            &putObjectDataCallback
        };

        S3MultipartCommitHandler commit_handler = {
            {
                    &responsePropertiesCallback,&responseCompleteCallback
            },
            &multipartPutXmlCallback,
            0
        };
        
        manager.etags = (char **) malloc(sizeof(char *) * totalSeq);
        manager.next_etags_pos = 0;
               
        if (uploadId) {
            manager.upload_id = strdup(uploadId);
            manager.remaining = contentLength;
            if(!try_get_parts_info(bucketName, key, &manager)) {
                fseek(data.infile, -(manager.remaining), 2);
                contentLength = manager.remaining;
                goto upload;
            }else {
                goto clean;
            }
        }
           
        do {
            S3_initiate_multipart(&bucketContext,key,0, &handler,0, &manager);
        } while (S3_status_is_retryable(statusG) && should_retry());

        if (manager.upload_id == 0 || statusG != S3StatusOK) {
            goto clean;
        }

upload: 
        todoContentLength -= MULTIPART_CHUNK_SIZE * manager.next_etags_pos;
        for (seq = manager.next_etags_pos + 1; seq <= totalSeq; seq++) {
            memset(&partData, 0, sizeof(MultipartPartData));
            partData.manager = &manager;
            partData.seq = seq;
            partData.put_object_data = data;
            partContentLength = ((contentLength > MULTIPART_CHUNK_SIZE) ?
                                 MULTIPART_CHUNK_SIZE : contentLength);
            partData.put_object_data.contentLength = partContentLength;
            partData.put_object_data.originalContentLength = partContentLength;
            partData.put_object_data.totalContentLength = todoContentLength;
            partData.put_object_data.totalOriginalContentLength = totalContentLength;
            putProperties.md5 = 0;
            do {
	        S3_upload_part(&bucketContext, key, &putProperties,
			   &putObjectHandler, seq, manager.upload_id,
			   partContentLength,0, &partData);
            } while (S3_status_is_retryable(statusG) && should_retry());
            if (statusG != S3StatusOK) {
                goto clean;
            }
            contentLength -= MULTIPART_CHUNK_SIZE;
            todoContentLength -= MULTIPART_CHUNK_SIZE;
        }
       
        int i;
        int size = 0;
        size += growbuffer_append(&(manager.gb), "<CompleteMultipartUpload>",
                                  strlen("<CompleteMultipartUpload>"));
        char buf[256];
        int n;
        for (i = 0; i < totalSeq; i++) {
            n = snprintf(buf, sizeof(buf), "<Part><PartNumber>%d</PartNumber>"
                         "<ETag>%s</ETag></Part>", i + 1, manager.etags[i]);
            size += growbuffer_append(&(manager.gb), buf, n);
        }
        size += growbuffer_append(&(manager.gb), "</CompleteMultipartUpload>",
                                  strlen("</CompleteMultipartUpload>"));
        manager.remaining = size;

        do {
            S3_complete_multipart_upload(&bucketContext, key, &commit_handler,
                                         manager.upload_id, manager.remaining,
                                         0,  &manager); 
        } while (S3_status_is_retryable(statusG) && should_retry());
        if (statusG != S3StatusOK) {
            goto clean;
        }

    clean:
        if(manager.upload_id) {
            free(manager.upload_id);
        }
        for (i = 0; i < manager.next_etags_pos; i++) {
            free(manager.etags[i]);
        }
        growbuffer_destroy(manager.gb);
        free(manager.etags);
    }
    
    S3_deinitialize();
    return 0;
}

typedef struct list_service_data
{
	int headerPrinted;
	int allDetails;
} list_service_data;

char *slist_cb_buckets[1024];
char *slist_cb_time[256];
int slist_cb_len = 0;

static S3Status listServiceCallback(const char *ownerId,
                                    const char *ownerDisplayName,
                                    const char *bucketName,
                                    int64_t creationDate, void *callbackData)
{
	char timebuf[256];
	if (creationDate >= 0) {
		time_t t = (time_t) creationDate;
		strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
	}
	else {
		timebuf[0] = 0;
	}

	slist_cb_buckets[slist_cb_len] = malloc(sizeof(char) * 1024);
	slist_cb_time[slist_cb_len] = malloc(sizeof(char) * 256);
	sprintf(slist_cb_buckets[slist_cb_len], "%s", bucketName);
	sprintf(slist_cb_time[slist_cb_len], "%s", timebuf);
	slist_cb_len++;
	return S3StatusOK;
}


static int list_service(int allDetails)
{
	list_service_data data;
	data.headerPrinted = 0;
	data.allDetails = allDetails;

	S3_init();
	S3ListServiceHandler listServiceHandler =
	{
		{ &responsePropertiesCallback, &responseCompleteCallback },
		&listServiceCallback
	};
	do {
		S3_list_service(
			protocolG, accessKeyIdG, secretAccessKeyG,
			0, 0, 0, &listServiceHandler, &data
		);
	} while (S3_status_is_retryable(statusG) && should_retry());

    if (statusG != S3StatusOK) {
	return 1;
    }
    S3_deinitialize();
    return 0;
}

typedef struct list_bucket_callback_data
{
	int isTruncated;
	char nextMarker[1024];
	int keyCount;
	int allDetails;
} list_bucket_callback_data;

char *blist_cb_buckets[1024];
char *blist_cb_time[256];
long blist_cb_size[1024];
int blist_cb_len = 0;

static S3Status listBucketCallback(int isTruncated, const char *nextMarker,
                                   int contentsCount,
                                   const S3ListBucketContent *contents,
                                   int commonPrefixesCount,
                                   const char **commonPrefixes,
                                   void *callbackData)
{
	list_bucket_callback_data *data =
		(list_bucket_callback_data *) callbackData;
	int i;

	data->isTruncated = isTruncated;
	if ((!nextMarker || !nextMarker[0]) && contentsCount) {
		nextMarker = contents[contentsCount - 1].key;
	}
	if (nextMarker) {
		snprintf(data->nextMarker, sizeof(data->nextMarker), "%s",
			nextMarker);
	}
	else {
		data->nextMarker[0] = 0;
	}

	for (i = 0; i < contentsCount; i++) {
	const S3ListBucketContent *content = &(contents[i]);
	char timebuf[256];
		time_t t = (time_t) content->lastModified;
		strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ",
			gmtime(&t));

		blist_cb_buckets[blist_cb_len] = malloc(sizeof(char) * 1024);
		blist_cb_time[blist_cb_len] = malloc(sizeof(char) * 256);

		sprintf(blist_cb_buckets[blist_cb_len], "%s", content->key);
		sprintf(blist_cb_time[blist_cb_len], "%s", timebuf);
		blist_cb_size[blist_cb_len] = content->size;

		blist_cb_len++;
	}
	data->keyCount += contentsCount;
	return S3StatusOK;
}

static int list_bucket(const char *bucketName, const char *prefix,
                        const char *marker, const char *delimiter,
                        int maxkeys, int allDetails)
{
	S3_init();
	S3BucketContext bucketContext =
	{
		0,
		bucketName,
		protocolG,
		uriStyleG,
		accessKeyIdG,
		secretAccessKeyG,
		0
	};

	S3ListBucketHandler listBucketHandler =
	{
		{ &responsePropertiesCallback, &responseCompleteCallback },
		&listBucketCallback
	};
	list_bucket_callback_data data;

	if (marker) {
		snprintf(data.nextMarker, sizeof(data.nextMarker), "%s", marker);
	}
	else {
		data.nextMarker[0] = 0;
	}
	data.keyCount = 0;
	data.allDetails = allDetails;

	do {
		data.isTruncated = 0;
		do {
			S3_list_bucket(&bucketContext, prefix, data.nextMarker,
				delimiter, maxkeys, 0, &listBucketHandler, &data);
		} while (S3_status_is_retryable(statusG) && should_retry());
		if (statusG != S3StatusOK) {
			break;
		}
	} while (data.isTruncated && (!maxkeys || (data.keyCount < maxkeys)));

	if (statusG != S3StatusOK) {
		return 1;
	}
	S3_deinitialize();
	return 0;
}

static int
init(struct lua_State *L)
{
	if (lua_gettop(L) < 4)
		luaL_error(L, "Usage: s3.init(access_key_id, secret_access_key, region_name, hostname)");

	accessKeyIdG = lua_tostring(L, 1);
	secretAccessKeyG = lua_tostring(L, 2);
	regionNameG = lua_tostring(L, 3);
	hostname= lua_tostring(L, 4);
	return 0;
}

static ssize_t
get_s3(va_list ap){
	const char *bucket = va_arg(ap, char*);
	const char *key = va_arg(ap, char*);
	const char *filename = va_arg(ap, char*);
	return get_object(bucket, key, filename);
}

static int
get(struct lua_State *L){
	if (lua_gettop(L) < 3)
		luaL_error(L, "Usage: s3.init(bucket, key, filename)");
	const char *bucket = lua_tostring(L, 1);
	const char *key = lua_tostring(L, 2);
	const char *filename = lua_tostring(L, 3);

	lua_pushinteger(L, coio_call(
		get_s3, bucket, key, filename
	));
	return 1;
}

static ssize_t
put_s3(va_list ap){
	const char *bucket = va_arg(ap, char*);
	const char *key = va_arg(ap, char*);
	const char *filename = va_arg(ap, char*);
	const char *hash = va_arg(ap, char*);
	return put_object(bucket, key, filename, hash);
}

static int
put(struct lua_State *L){
	if (lua_gettop(L) < 4)
		luaL_error(L, "Usage: s3.init(bucket, key, filename, hash)");
	const char *bucket = lua_tostring(L, 1);
	const char *key = lua_tostring(L, 2);
	const char *filename = lua_tostring(L, 3);
	const char *hash = lua_tostring(L, 4);

	lua_pushinteger(L, coio_call(
		put_s3, bucket, key, filename, hash
	));
	return 1;
}

static int
lua_list_bucket_success(struct lua_State *L){
	int i;
	lua_createtable(L, blist_cb_len, 0);
        for(i = 0;i < blist_cb_len;i++){
		/* Create nested table {name=..., date=..., size=...}*/
		lua_createtable(L, 4, 0);

		lua_pushstring(L, blist_cb_buckets[i]);
		lua_setfield(L, -2, "name");

		lua_pushstring(L, blist_cb_time[i]);
		lua_setfield(L, -2, "date");

		lua_pushinteger(L, blist_cb_size[i]);
		lua_setfield(L, -2, "size");

		lua_rawseti(L, -2, i+1);
	}
	return 1;
}

static int
lua_list_bucket(struct lua_State *L){
	if (lua_gettop(L) < 1)
		luaL_error(L, "Usage: libs3.list_bucket(bucket_name)");
	const char *bucket = lua_tostring(L, 1);
	blist_cb_len = 0;
	if(list_bucket(bucket, 0, 0, 0, 0, 0)){
		return 0;
	}
        return lua_list_bucket_success(L);
}

static int
lua_list_bucket_prefix(struct lua_State *L){
	if (lua_gettop(L) < 2)
		luaL_error(L, "Usage: libs3.list_bucket_prefix(bucket_name, prefix)");
	const char *bucket = lua_tostring(L, 1);
	const char *prefix = lua_tostring(L, 2);
	blist_cb_len = 0;
	if(list_bucket(bucket, prefix, 0, 0, 0, 0)){
		return 0;
	}
        return lua_list_bucket_success(L);
}

static int
lua_list_service(struct lua_State *L){
	slist_cb_len = 0;
        if(list_service(0)){
		return 0;
	}
	int i;
	lua_createtable(L, slist_cb_len, 0);
        for(i = 0;i < slist_cb_len;i++){
		/* Create nested table {name=..., date=...}*/
		lua_createtable(L, 2, 0);

		lua_pushstring(L, slist_cb_buckets[i]);
		lua_setfield(L, -2, "name");

		lua_pushstring(L, slist_cb_time[i]);
		lua_setfield(L, -2, "date");

		lua_rawseti(L, -2, i+1);
	}
	return 1;
}

LUA_API int
luaopen_s3_cfunctions(lua_State *L)
{
	/* result is returned from require('s3.cfunctions') */
	lua_newtable(L);
	static const struct luaL_Reg meta [] = {
		{"init", init},
		{"get", get},
		{"put", put},
		{"list_service", lua_list_service},
		{"list_bucket", lua_list_bucket},
		{"list_bucket_prefix", lua_list_bucket_prefix},
		{NULL, NULL}
	};
	luaL_register(L, NULL, meta);
	return 1;
}
/* vim: syntax=c ts=8 sts=8 sw=8 noet */
