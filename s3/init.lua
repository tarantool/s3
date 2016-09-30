-- Lua/C submodule
local libs3 = require('s3.cfunctions')
local fio = require('fio')
local digest = require('digest')
local crypto = require('crypto')
local log = require('log')
local CHUNK_SIZE = 1024 * 10

local s3 = {
    options = {'access', 'secret', 'region', 'host'},
    valid_opts = function(self, opts)
        local is_valid = true
        for _, k in pairs(self.options) do
            if opts[k] == nil or type(opts[k]) ~= 'string' then
                is_valid = false
            end
        end
        return is_valid
    end,

    init = function(self, opts) --access_key, secret_key, region, host)
        if not self:valid_opts(opts) then
            return false
        end
        libs3.init(opts.access, opts.secret, opts.region, opts.host)

        -- libs3.get wrapper
        self.download_file = function(self, bucket, key, filename)
            if libs3.get(bucket, key, filename) ~= 0 then
                return false
            end
            return fio.stat(filename) ~= nil
        end

        self.list = function(self, bucket)
            if bucket == nil then
                return libs3.list_service()
            end
            return libs3.list_bucket(bucket)
        end

        -- libs3.put wrapper
	self.upload_file = function(self, bucket, key, filename)
            if fio.stat(filename) == nil then
                return false
            end

            local md5, sha256 = self.get_hash(filename)
            local hash = md5 .. ":" .. sha256
            return libs3.put(bucket, key, filename, hash) == 0
        end
    end,
    get_hash = function(filename)
        local file = fio.open(filename, {'O_RDONLY'})
        local md5_worker = crypto.digest.md5:new()
        local sha256_worker = crypto.digest.sha256:new()

	function tohex(str)
	    return (str:gsub('.', function (c)
		return string.format('%02x', string.byte(c))
	    end))
	end

        local data = file:read(CHUNK_SIZE)
        while data ~= '' do
            md5_worker:update(data)
            sha256_worker:update(data)
            data = file:read(CHUNK_SIZE)
        end
        file:close()

        -- required by s3: base64(binary md5) and sha256 hex
        local md5 = digest.base64_encode(md5_worker:result())
        local sha256 = tohex(sha256_worker:result())
	return md5, sha256
    end,
}
return s3
-- vim: ts=4 sts=4 sw=4 et
