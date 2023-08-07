local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local max_page = 0
local is_remix = false

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    local a, b = string.match(item, "^([^:]+):(.+)$")
    if a and b and a == "post" then
      discover_item(target, "post-api:" .. b)
    end
--print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  local value = string.match(url, "^https?://api%.skyrock%.com/v2/user/get%.json%?id_user=([0-9]+)$")
  local type_ = "blog-api"
  if not value then
    other, value = string.match(url, "^https?://api%.skyrock%.com/v2/blog/get_post%.json%?id_user=[0-9]+&id_post=([0-9]+)$")
    type_ = "post-api"
  end
  if not value then
    value = string.match(url, "^https?://www%.skyrock%.com/common/r/skynautes/card/([0-9]+)$")
    type_ = "blog"
  end
  if not value then
    other, value = string.match(url, "^https?://([^%.]+)%.skyrock%.com/([0-9]+)$")
    type_ = "post"
  end
  if not value then
    other, other2, value = string.match(url, "^https?://([^%.]+)%.skyrock%.com/profil/photos/([0-9]+)/([0-9]+)$")
    type_ = "photo"
  end
  if not value then
    other, other2, value = string.match(url, "^https?://([^%.]+)%.skyrock%.com/profil/videos/([0-9]+)/([0-9]+)$")
    type_ = "video"
  end
  if not value then
    value = string.match(url, "^https?://([^/]*skyrock%.net/.+)$")
    type_ = "asset"
  end
  if not value then
    other, value = string.match(url, "^https?://([^%.]+)%.skyrock%.com/tags/([^%.]+)%.html$")
    type_ = "tag"
  end
  if value then
    return {
      ["value"]=value,
      ["type"]=type_,
      ["other"]=other,
      ["other2"]=other2
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    item_type = found["type"]
    item_value = found["value"]
    if string.match(item_type, "^post") or item_type == "tag" then
      item_user = found["other"]
      item_name_new = item_type .. ":" .. item_user .. ":" .. item_value
    elseif item_type == "photo" or item_type == "video" then
      item_user = found["other"]
      item_name_new = item_type .. ":" .. item_user .. ":" .. found["other2"] .. ":" .. item_value
    else
      item_name_new = item_type .. ":" .. item_value
    end
    if item_name_new ~= item_name then
      ids = {}
      ids[item_value] = true
      abortgrab = false
      tries = 0
      max_page = 0
      is_remix = false
      retry_url = false
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url]
    or string.match(url, "^https?://mg.skyrock.com/.") then
    return true
  end

  if (item_type ~= "www" and string.match(url, "^https?://www%.skyrock%.com/"))
    or string.match(url, "^https?://[^/]*skyrock%.com/common/r/social/")
    or string.match(url, "^https?://[^/]*skyrock%.com/.*[%?&]connect=1")
    or string.match(url, "^https?://[^/]+/%*$")
    or string.match(url, "/profil/profil%-comments/[0-9]")
    or string.match(url, "/honors/.*[%?&]id_badge=")
    or string.match(url, "/common/r/friends/follow/")
    or string.match(url, "/common/r/blog/subscribe/")
    or string.match(url, "/common/r/stats/social_share")
    or string.match(url, "/profil/photos/blog/")
    or string.match(url, "/profil/videos/blog/")
    or string.match(url, "[%?&]action=ADD_COMMENT")
    or (
      string.match(url, "/common/r/skynautes/card/")
      and string.match(url, "/common/r/skynautes/card/([0-9]+)") ~= item_value
    ) then
    return false
  end

  local found = false
  for pattern, type_ in pairs({
    ["^https?://([^%.]+)%.skyrock%.com/([0-9]+)[^0-9a-zA-Z][^/]*$"]="post",
    ["^https?://([^%.]+)%.skyrock%.com/([0-9]+)$"]="post",
    ["^https?://([^%.]+)%.skyrock%.com/photo%.html%?.*id_article=([0-9]+)"]="post",
    ["^https?://([^%.]+)%.skyrock%.com/article_([0-9]+)%.html$"]="post",
    ["^https?://([^%.]+)%.skyrock%.com/profil/photos/([0-9]+)/([0-9]+)$"]="photo",
    ["^https?://([^%.]+)%.skyrock%.com/profil/videos/([0-9]+)/([0-9]+)$"]="video",
    ["^https?://([^/]*skyrock%.net/.+)$"]="asset",
    ["^https?://([^%.]+)%.skyrock%.com/tags/([^%.]+)%.html"]="tag"
  }) do
    local match = nil
    if string.match(type_, "^post") or type_ == "tag" then
      match, other = string.match(url, pattern)
    elseif type_ == "photo" or type_ == "video" then
      match, other, other2 = string.match(url, pattern)
    else
      match = string.match(url, pattern)
    end
    if match
      and not (
        string.match(type_, "^post")
        and tonumber(other) <= max_page
      ) then
      if string.match(type_, "^post") or type_ == "tag" then
        match = match .. ":" .. other
      elseif type_ == "photo" or type_ == "video" then
        match = match .. ":" .. other .. ":" .. other2
      end
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        if string.match(type_, "^([^%-]+)") ~= string.match(item_type, "^([^%-]+)")
          or match ~= string.match(item_name, "^[^:]+:(.+)$") then
          found = true
        end
      end
    end
  end
  if found then
    return false
  end

  if item_type == "post"
    and is_remix
    and string.match(url, "[%?&]action=SHOW_[A-Z]") then
    return false
  end

  if string.match(url, "^https?://[^/]*sk%.mu/")
    or ids[string.match(url, "^https?://([^%.]+)%.skyrock%.com/")] then
    return true
  end

  if string.match(url, "^https?://[^/]*skyrock%.com/") then
    for _, pattern in pairs({
      "([0-9]+)",
      "([^%./]+)",
      "([^%./_]+)"
    }) do
      for s in string.gmatch(string.match(url, "^https?://[^/]+(/.*)"), pattern) do
        if ids[s] then
          return true
        end
      end
    end
  end

  if not string.match(url, "^https?://[^/]*skyrock%.com/") then
    discover_item(discovered_outlinks, url)
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"]) and (
    not processed(url)
    or string.match(url, "/common/r/skynautes/card/")
    or string.match(url, "^https?://[^%.]+%.skyrock%.com/[0-9]+$")
  ) and string.match(url, "^https://") and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  --[[local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end]]

  local function fix_case(newurl)
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    --newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 then
      return nil
    end
    local a, b = string.match(newurl, "^(https?)(:.+)$")
    if a == "http" then
      newurl = "https" .. b
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      if string.match(url_, "/profil/wall/more%?")
        or string.match(url_, "/common/r/skynautes/card/") then
        table.insert(urls, {
          url=url_,
          headers={
            ["X-Requested-With"]="XMLHttpRequest"
          }
        })
      else
        table.insert(urls, {
          url=url_
        })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    --newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    --newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function check_new_params(newurl, param, value)
    if string.match(newurl, "[%?&]" .. param .. "=") then
      newurl = string.gsub(newurl, "([%?&]" .. param .. "=)[^%?&;]+", "%1" .. value)
    else
      if string.match(newurl, "%?") then
        newurl = newurl .. "&"
      else
        newurl = newurl .. "?"
      end
      newurl = newurl .. param .. "=" .. value
    end
    check(newurl)
  end

  local function queue_next(url, param, default)
    if not default then
      default = 1
    end
    local num = string.match(url, "[%?&]" .. param .. "=([0-9]+)")
    if num then
      num = tonumber(num) + 1
    else
      num = default
    end
    num = tostring(num)
    check_new_params(url, param, num)
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  if allowed(url)
    and status_code < 300
    and not string.match(url, "^https?://[^/]*skyrock%.net/")
    and not string.match(url, "^https?://mg.skyrock.com/.") then
    html = read_file(file)
    if item_type == "post" then
      check("https://" .. item_user .. ".skyrock.com/article_" .. item_value .. ".html")
      check("https://" .. item_user .. ".skyrock.com/" .. item_value)
      check("https://" .. item_user .. ".skyrock.com/" .. item_value .. ".html")
      local slug = string.match(url, "^https?://[^%.]+%.skyrock%.com/" .. item_value .. "(.*)%.html$")
      if slug then
        local remix_slug = string.match(html, '<p%s+class="remix_source">%s*<a%s+href="https?://[^%.]+%.skyrock%.com/[0-9]+([^"]+)%.html">Remix</a>')
        if remix_slug and remix_slug == slug then
          is_remix = true
        end
      end
    end
    if item_type == "blog" then
      if string.match(url, "^https?://[^%.]+%.skyrock%.com/.") then
        check(string.match(url, "^(https?://[^%.]+%.skyrock%.com/)"))
      elseif string.match(url, "^https?://[^%.]+%.skyrock%.com/$") then
        check(url .. "robots.txt")
        check(url .. "atom.xml")
        check(url .. "sitemap.xml")
        for a, b, c in string.gmatch(html, '<option%s+value="([0-9]+)">Page%s+([0-9]+)%s+of%s+([0-9]+)</option>') do
          if a ~= b then
            error("Page numbers do not match.")
          end
          c = tonumber(c)
          if c > max_page then
            max_page = c
          end
        end
      end
    end
    if string.match(url, "^https?://api%.skyrock%.com") then
      local json = cjson.decode(html)
      if string.match(url, "/v2/user/get%.json%?id_user=") then
        local user_url = json["user_url"]
        local user_name = string.match(user_url, "^https?://([^%.]+)%.skyrock%.com/$")
        ids[user_name] = true
        for _, endpoint in pairs({
          "/blog/get.json",
          "/user/get.json",
          "/profile/get.json",
          "/blog/list_posts.json",
          "/profile/list_albums.json",
          "/profile/get_tags.json",
          "/profile/get_tags_types.json",
          "/profile/get_background.json",
          "/mood/list_moods.json",
          "/mood/get_mood.json",
          "/relationship/list_relations.json",
          "/relationship/list_relations_ids.json",
          "/relationship/list_online_relations.json"
        }) do
          local newurl = urlparse.absolute(url, "//api.skyrock.com/v2" .. endpoint)
          for _, params in pairs({
            "id_user=" .. item_value,
            "username=" .. user_name
          }) do
            local newurl2 = newurl .. "?" .. params
            if string.match(endpoint, "/relationship/") then
              for _, kind in pairs({
                "friend",
                "following",
                "followed_by"
              }) do
                check(newurl2 .. "&kind=" .. kind)
              end
            end
            check(newurl2)
          end
        end
      elseif string.match(url, "/v2/blog/get_post%.json%?.*id_post=") then
        local user_name = string.match(user_url, "^https?://([^%.]+)%.skyrock%.com/")
        for _, name in pairs({
          "get_post",
          "list_post_medias",
          "list_post_comments",
          "list_pictures"
        }) do
          local newurl = urlparse.absolute(url, "//api.skyrock.com/v2/blog/" .. name .. ".json")
          check(newurl .. "?id_user=" .. item_user .. "&id_post=" .. item_value)
          check(newurl .. "?username=" .. user_name .. "&id_post=" .. item_value)
        end
      elseif string.match(url, "/profile/list_albums%.json") then
        local user_name = string.match(url, "username=([^&]+)")
        for _, data in pairs(json) do
          check("https://api.skyrock.com/v2/profile/list_pictures.json?id_user=" .. item_value .. "&id_album=" .. tostring(data["id_album"]))
          if user_name then
            check("https://api.skyrock.com/v2/profile/list_pictures.json?username=" .. user_name .. "&id_album=" .. tostring(data["id_album"]))
          end
        end
      end
      local max_page = json["max_page"]
      if max_page then
        for i=1,json["max_page"] do
          check_new_params(url, "page", tostring(i))
        end
      end
      if string.match(url, "/list_pictures%.json") then
        for _, data in pairs(json) do
          check("https://" .. item_value .. ".skyrock.com/profil/photos/" .. tostring(id_album) .. "/" .. tostring(id_picture))
        end
        queue_next(url, "page")
      end
      html = html .. flatten_json(json)
    end
    html = string.gsub(html, "\\", "")
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  if string.match(url["url"], "^https?://api%.skyrock%.com/")
    or string.match(url["url"], "/profil/wall/more%?")
    or string.match(url["url"], "/common/r/skynautes/card/[0-9]+%?")
    or (
      string.match(url["url"], "/common/r/skynautes/card/[0-9]")
      and string.match(url["url"], "/common/r/skynautes/card/([0-9]+)$") ~= item_value
    ) then
    local html = read_file(http_stat["local_file"])
    if not (string.match(html, "^%s*{") and string.match(html, "}%s*$"))
      and not (string.match(html, "^%s*%[") and string.match(html, "%]%s*$")) then
      print("Did not get JSON data.")
      retry_url = true
      return false
    end
    local json = cjson.decode(html)
    if (
      string.match(url["url"], "/profil/wall/more%?")
      or string.match(url["url"], "/common/r/skynautes/card/")
    ) and not json["success"] then
      retry_url = true
      return false
    end
  elseif item_type ~= "asset"
    and status_code == 200
    and not string.match(url["url"], "%.xml")
    and not string.match(url["url"], "%.txt")
    and not string.match(url["url"], "^https?://mg.skyrock.com/.") then
    local html = read_file(http_stat["local_file"])
    if not string.match(html, "</html>") then
      print("Bad HTML.")
      retry_url = true
      return false
    end
  end
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 404
    and not (
      (
        string.match(url["url"], "/common/r/skynautes/card/")
        or string.match(url["url"], "^https?://sk%.mu/")
        or string.match(url["url"], "^https?://[^%.]+%.skyrock%.com/[0-9]+$")
      )
      and http_stat["statcode"] == 302
    ) then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if item_type == "blog"
      and string.match(url["url"], "/common/r/skynautes/card/" .. item_value) then
      local username = string.match(newloc, "^https?://([^%.]+)%.skyrock%.com/")
      if username then
        ids[username] = true
      else
        abort_item()
      end
    end
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code < 400 then
    downloaded[url["url"]] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    if tries > 5 then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 10
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["skyblog-rqb4kpprgnb9mdfq"] = discovered_items,
    ["urls-73c894a89ehhbiwt"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


