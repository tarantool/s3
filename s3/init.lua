-- Lua/C submodule
local libs3 = require('s3.cfunctions')
local fio = require('fio')
local digest = require('digest')
local log = require('log')
local CHUNK_SIZE = 1024 * 10

local s3 = {
    init = function(self, access_key, secret_key, region, host)
        libs3.init(access_key, secret_key, region, host)

        -- libs3.get wrapper
        self.download_file = function(self, bucket, key, filename)
            if libs3.get(bucket, key, filename) ~= 0 then
                return false
            end
            return fio.stat(filename) ~= nil
        end

        -- libs3.put wrapper
	self.upload_file = function(self, bucket, key, filename)
            if fio.stat(filename) == nil then
                return false
            end
            local file = self.readfile(filename)

            -- required by s3: base64(binary md5) and sha256 hex
            local md5 = digest.base64_encode(digest.md5(file))
            local sha256 = digest.sha256_hex(file)
            local hash = md5 .. ":" .. sha256
            return libs3.put(bucket, key, filename, hash) == 0
        end
    end,
    readfile = function(filename)
        local result = ''
        local file = fio.open(filename, {'O_RDONLY'})

        -- FIXME: this is naive way for large files
        local data = file:read(CHUNK_SIZE)
        while data ~= '' do
            result = result .. data
            data = file:read(CHUNK_SIZE)
        end
        file:close()
	return result
    end,
}
return s3
-- vim: ts=4 sts=4 sw=4 et
