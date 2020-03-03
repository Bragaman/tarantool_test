#!/usr/bin/env tarantool

log = require('log')
json = require('json')

-- настроить базу данных
box.cfg {
    listen = 'localhost:3301'
}

box.once("init", function()
    box.schema.space.create('data')
    box.space.data:format({
        {name = 'key', type='string'},
        {name = 'value', type='string'}
    })
    box.space.data:create_index('primary',
            { type = 'HASH', parts = {1, 'string'}})
end)

function json_response(req, status_code, value)
    local body = json.encode({info = value})
    log_helper(req, status_code, body)
    return {
        status = status_code,
        body = body
    }
end

function log_helper(req, status_code, msg)
    local log_msg = string.format(
            "%s %s %d response: %s",
            req:method(), req:path(), status_code, msg
    )
    if status_code >= 400 and status_code < 200 then
        log.error(log_msg)
    else
        log.info(log_msg)
    end
end

function validate_req_key(req)
    local is_valid, result = pcall(function()
        return req:post_param("key")
    end)
    if not is_valid then
        return false, json_response(req, 400, result)
    end
    if result == nil then
        return false, json_response(req, 400, "Key \"key\" can not be nil")
    end
    return true, result
end

function validate_req_value(req)
    local is_valid, result = pcall(function()
        return req:post_param("value")
    end)
    if not is_valid then
        return false, json_response(req, 400, result)
    end
    if result == nil then
        return false, json_response(req, 400, "Key \"value\" can not be nil")
    end
    local value
    is_valid, value = pcall(function()
        return json.encode(result)
    end)
    if not is_valid then
        return false, json_response(req, 400, string.format("Value is invalid: %s", value))
    end
    return true, value
end

function get(req)
    local key = req:stash('key')
    local value_tuple = box.space.data:get(key)

    if value_tuple == nil then
        return json_response(req, 404, "Not found")
    end
    local body = json.encode({value = json.decode(value_tuple[2])})
    log_helper(req, 200, body)
    return {
        status = 200,
        body = body
    }
end

function add(req)
    local is_key, result_key = validate_req_key(req)
    if not is_key then
        return result_key
    end
    local is_valid_value, result_value = validate_req_value(req)
    if not is_valid_value then
        return result_value
    end
    local data = box.space.data
    local is_valid, result = pcall(function ()
        return data:insert{ result_key, result_value}
    end)

    if is_valid then
        return json_response(req, 201, "Created")
    elseif result.code == box.error.TUPLE_FOUND then
        return json_response(req, 409,
                string.format("Key \"%s\" already exists", result_key))
    else
        return json_response(req, 500,
                string.format("Unexpected error: %s", result))
    end
end

function update(req)
    local key = req:stash('key')
    local is_valid_value, result_value = validate_req_value(req)
    if not is_valid_value then
        return result_value
    end
    local is_valid, result = pcall(function ()
        return box.space.data:update(key, {{'=', 2, result_value}})
    end)

    if is_valid then
        if result == nil then
            return json_response(req, 404, "Not found")
        end
        return json_response(req, 200, "Updated")
    end
    return json_response(req, 500, string.format("Unexpected error: %s", result))
end

function delete(req)
    local key = req:stash('key')
    local data = box.space.data
    local is_valid, result = pcall(function ()
        return data:delete(key)
    end)

    if is_valid then
        if result == nil then
            return json_response(req, 404, "Not found")
        end
        return json_response(req, 204, "Deleted")
    end
    return json_response(req, 500, string.format("Unexpected error: %s", result))
end

server = require('http.server').new('localhost', '8080')
router = require('http.router').new({charset = 'application/json'})

router:route({ path = '/kv/:key', method = 'GET'}, get)
router:route({ path = '/kv/', method = 'POST'}, add)
router:route({ path = '/kv/:key', method = 'PUT'}, update)
router:route({ path = '/kv/:key', method = 'DELETE'}, delete)

server:set_router(router)
server:start()