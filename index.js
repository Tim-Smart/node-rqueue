/**
 * Requires.
 */
var redis  = require('node-redis')
  , Filter = require('filter')
  , uuid   = require('node-uuid')
  , util   = require('util')

// Function for map calls
var mapToString = function (id) { return id.toString() }

/**
 * Log for a async callback
 *
 * @param {Error} error
 * @param {Mixed} data
 */
function asyncLog (error, data) {
  if (error) return console.error(error)
  console.log(data)
}

/**
 * The Queue prototype used by the server to add jobs to a queue.
 *
 * @constructor
 * @param {Object} options: A hash that can contain name, host, port, auth, prefix
 */
var Queue = function (options) {
  Filter.call(this)

  var queue         = this

  queue.id          = uuid()
  queue.name        = options.name
  queue.prefix      = options.prefix || ''
  queue.host        = options.host
  queue.port        = options.port
  queue.auth        = options.auth

  queue.timeout     = options.timeout || null
  queue.concurrency = options.concurrency || 1
  queue.processing  = 0
  queue._prefix     = ''

  queue.client      = redis.createClient(queue.port, queue.host, queue.auth)
  queue.bclient     = null
  queue.sclient     = null

  queue._onError    = function onError (error) {
    if (error) {
      queue.emit('error', error)
    }
  }

  // On blpop
  queue._onPop       = function onPop (error) {
    if (error) return queue._done(error, true)
    queue._next()

    queue.client.watch(queue._prefix + ':queued')
    queue.client.zrevrange(queue._prefix + ':queued', '0', '0', gotQueued)
  }

  // On zrevrange
  function gotQueued (error, job_id) {
    if (error) return queue._done(error, true)
    if (!job_id || !job_id[0]) return queue._done()

    queue.client.hget(queue._prefix + ':jobs', job_id[0].toString(), gotJob)
  }

  function gotJob (error, data) {
    if (error) return queue._done(error, true)

    try {
      queue._processJob(queue._returnJob(data))
    } catch (error) {
      return queue._done(error, true)
    }
  }

  queue.client.on('error', queue._onError)
  queue.refreshKeys()
}

// Inherits from Filter.
Queue.prototype.__proto__ = Filter.prototype

/**
 * Creates a new Queue object.
 *
 * @param {Object} options: A hash that can contain name, host, port, auth, prefix
 * @returns {Queue}
 */
exports.createQueue = function (options) {
  return new Queue(options)
}

exports.Queue = Queue

/**
 * Regenerate keys from current prefix and name
 */
Queue.prototype.refreshKeys = function refreshKeys () {
  var queue         = this
  queue._prefix     = queue.prefix + 'rq:' + queue.name
  return this
}

/**
 * If a previous worker crashed, migrate its running jobs back to
 * the queue.
 *
 * @param {String} id
 * @param @optional {Function} callback
 */
Queue.prototype.migrate = function migrate (id, callback) {
  var queue   = this
  var client  = queue.client

  function onJobIds (error, ids) {
    if (error) {
      if (callback) return callback(error)
      return queue.emit('error', error)
    }
    if (!ids || 0 === ids.length) {
      if (callback) return callback()
      return
    }

    ids = ids.map(mapToString)
    ids.unshift(queue._prefix + ':jobs')

    client.hmget(ids, gotJobs)
  }

  function gotJobs (error, jobs) {
    if (error) {
      if (callback) return callback(error)
      return queue.emit('error', error)
    }

    client.multi()

    for (var i = 0, il = jobs.length; i < il; i++) {
      try {
        queue
          ._returnJob(jobs[i])
          .addMessage('Retry set on "' + queue.name + ':' + queue.id + '".')
          .forward('queued', null, true)
      } catch (error) {}
    }

    client.del(queue._prefix + ':running:' + id)
    client.exec(callback)
  }

  client.zrange(queue._prefix + ':running:' + id, '0', '-1', onJobIds)

  return queue
}

/**
 * Adds a new job to the queue.
 *
 * @param {Object} payload: The data payload to enqueue.
 * @param {Function} callback
 */
Queue.prototype.write = function write (payload, callback) {
  var queue       = this
  var job         = queue.createJob(payload)

  job.forward('queued', callback)

  return job
}

/**
 * Create a job and call save / forward manually.
 *
 * @param {Object} payload
 */
Queue.prototype.createJob = function createJob (payload) {
  var queue       = this
  var now         = Date.now()
  var job         = new Job(
    queue
  , { id          : uuid()
    , status      : null
    , priority    : 1
    , timeout     : queue.timeout
    , retries     : 0
    , msg_count   : 0
    , msgs        : []
    , payload     : payload
    , created     : now
    , updated     : null
    }
  , true
  )

  return job
}

/**
 * Get a job from uuid
 *
 * @param {String} uuid
 * @param {Function} callback
 */
Queue.prototype.getJob = function getJob (uuid, callback) {
  if (!callback) callback = asyncLog
  var queue = this

  function onData (error, data) {
    if (error) return callback(error)

    queue._returnJob(data, callback)
  }

  queue.client.hget(queue._prefix + ':jobs', uuid, onData)

  return queue
}

/**
 * Return a job from raw redis data
 *
 * @param {Buffer} data
 * @param {Function} callback
 */
Queue.prototype._returnJob = function returnJob (data, callback) {
  var queue = this

  try {
    data    = JSON.parse(data.toString())
  } catch (error) {
    if (callback) return callback(error)
    throw error
  }

  var job   = new Job(queue, data)

  if (callback) callback(null, job)
  return job
}

/**
 * Overwrite resume to call _next
 */
Queue.prototype.resume = function resume () {
  var queue     = this
  queue.paused  = false
  queue._next()

  return true
}

/**
 * Look for jobs
 */
Queue.prototype._next = function next () {
  var queue = this

  if (queue.paused || !queue.writable) return

  if (queue.processing >= queue.concurrency) {
    return queue
  }

  ;++queue.processing

  queue.bclient.blpop(queue._prefix + ':listen', '0', queue._onPop)

  return queue
}

/**
 * Done a job, or errored before we got one.
 *
 * @param @optional {Error} error
 */
Queue.prototype._done = function done (error, premature) {
  var queue = this

  ;--queue.processing
  if (error) {
    queue.emit('error', error)
  }
  if (premature) {
    queue.client.rpush(queue._prefix + ':listen', '1')
  }

  queue._next()

  return queue
}

/**
 * Process a job. At this stage watch has been called on the queued sorted set
 * and we have a job id.
 */
Queue.prototype._processJob = function processJob (job) {
  var queue   = this
  var client  = queue.client
  var status  = 'running:' + queue.id
  var now     = Date.now()

  job.status  = status
  job.updated = now

  client.multi()
  client.zrem(queue._prefix + ':queued', job.id)
  client.zadd(queue._prefix + ':' + status, now, job.id)
  client.hset(queue._prefix + ':jobs', job.id, JSON.stringify(job))
  client.publish(queue._prefix + ':save' , job.id)
  client.publish(queue._prefix + ':running' , job.id)

  client.exec(function (error, results) {
    if (error) return queue._done(error, true)
    if (results === null) return queue._done(null, true)

    if (job.timeout) {
      job._timer = setTimeout(job._onTimeout, job.timeout, job)
    }

    queue.emit('data', job)
  })
}

/**
 * Start the queue listener.
 *
 * @param @optional {String} id
 * @param @optional {Function} callback
 */
Queue.prototype.listen = function listen (callback) {
  var queue     = this

  queue.bclient = redis.createClient(queue.port, queue.host, queue.auth)
  queue.bclient.on('error', queue._onError)
  queue.started = new Date()

  queue.client.zadd(
    queue._prefix + ':workers'
  , queue.started.getTime()
  , queue.id
  , function (error) {
      if (error) return queue.emit('error', error)
      if (callback) callback()
      queue._next()
    }
  )

  return queue
}

/**
 * Stop yol horses. Shut her down!
 * You should always call this if you want to register the queue
 * before shutting down your program.
 */
Queue.prototype.end = function end (callback) {
  var queue     = this

  if (queue.bclient) {
    queue.bclient.destroy()
    queue.bclient = null
  }
  if (queue.sclient) {
    queue.sclient.destroy()
    queue.sclient = null
  }
  queue.client.zrem(queue._prefix + ':workers', queue.id)
  queue.client.end(callback)

  Filter.prototype.end.call(queue)

  return queue
}

/**
 * Job prototype used by the workers.
 *
 * @constructor
 * @param {Object} parent: Parent instance
 * @param {Object} payload: The data to set as the payload.
 */
var Job = function (parent, data, is_new) {
  var job         = this

  job.id          = data.id
  job.status      = data.status
  job.priority    = +data.priority
  job.timeout     = data.timeout ? +data.timeout : null
  job.payload     = data.payload
  job.retries     = data.retries
  job.msg_count   = data.msg_count
  job.msgs        = data.msgs
  job.created     = data.created || Date.now()
  job.updated     = data.updated

  job.is_new      = is_new || false
  job.old_status  = job.status
  job._timer      = null

  job.prefix      = parent.prefix
  job._prefix     = parent._prefix
  job.queue       = parent.name

  // Associate with a redis client.
  job.parent      = parent
  job.client      = parent.client
}

exports.Job = Job

/**
 * Called when a job times out.
 * This function is usually not called with job set to this.
 *
 * @param {Job} job
 */
Job.prototype._onTimeout = function onTimeout (job) {
  job._timer  = null
  job.forward('timeout')
  job.parent._done()

  return job
}

/**
 * Add an message to the job.
 *
 * @param {String} message
 */
Job.prototype.addMessage = function addMessage (message) {
  var job = this

  ;++job.msg_count
  job.msgs.push('INFO: ' + message)

  return job
}

/**
 * Add an error to the job.
 *
 * @param {Error} error: The error object to add.
 */
Job.prototype.addError = function addError (error) {
  var job = this

  ;++job.msg_count
  job.msgs.push('ERROR: ' + (error.message || error.toString()))

  return job
}

/**
 * For JSON.stringify
 *
 * @return {Object}
 */
Job.prototype.toJSON = function () {
  var job       = this
  return (
    { id        : job.id
    , status    : job.status
    , priority  : job.priority
    , retries   : job.retries
    , msg_count : job.msg_count
    , msgs      : job.msgs
    , timeout   : job.timeout
    , payload   : job.payload
    , created   : job.created
    , updated   : job.updated
    }
  )
}

/**
 * Remove the job from redis
 *
 * @param {Function} callback
 * @param {Boolean} @optional multi : Are we already in a multi?
 */
Job.prototype.remove = function remove (callback, multi) {
  var job     = this
  var client  = job.client

  if (!multi) client.multi()
  client.hdel(job._prefix + ':jobs', job.id)
  if (job.status) client.zrem(job._prefix + ':' + job.status, job.id)
  client.publish(job._prefix + ':remove', job.id)
  if (!multi) client.exec(callback)

  return job
}

/**
 * Save a job.
 *
 * @param {Function} callback
 */
Job.prototype.save = function save (callback) {
  var job     = this
  var client  = job.client

  job.updated = Date.now()

  if (job.is_new) {
    job.addMessage('Created on "' + job.queue + ':' + job.parent.id + '".')
    client.publish(job._prefix + ':new', job.id)
  }

  // We won't do a multi here, as usually it has been called elsewhere.
  client.hset([job._prefix + ':jobs', job.id, JSON.stringify(job)], callback)
  client.publish(job._prefix + ':save' , job.id)

  return job
}

/**
 * Forward job to another status set.
 *
 * @param {String} dest
 * @param {Function} callback
 * @param {Boolean} multi : Already in a multi?
 */
Job.prototype.forward = function forward (dest, callback, multi) {
  var job     = this
  var client  = job.client
  var score   = 0

  if (!callback) {
    callback  = job.parent._onError
  }

  if (!multi) client.multi()

  if (!job.is_new) {
    client.zrem(job._prefix + ':' + job.status, job.id)
  }

  job.status  = dest
  job.save()
  score       = job.updated

  // Queued items need to be listened for, and sorted set score assigned the
  // job priority.
  if ('queued' === job.status) {
    score     = job.priority
    client.rpush(job._prefix + ':listen', '1')
  }

  client.zadd(job._prefix + ':' + job.status, score, job.id)
  client.publish(job._prefix + ':' + job.status, job.id)

  if (!multi) {
    function onExec (error, data) {
      if (error) return callback(error)
      callback(null, job)
    }

    client.exec(onExec)
  }

  return job
}

/**
 * Emit a event, like progress for example.
 *
 * @param {String} event
 * @param {String} message
 */
Job.prototype.emit = function emit (event, message, callback) {
  var job = this

  job.client.publish(
    [job._prefix + ':' + event + ':' + job.id, message]
  , callback
  )

  return job
}

/**
 * Re-process the job by adding back to the queue.
 *
 * @param {Function} callback: The optional callback
 */
Job.prototype.retry = function (error, callback) {
  var job = this
  if (job.status === 'timeout') return false

  if (job._timer) clearTimeout(job._timer)

  if ('function' === typeof error) {
    callback  = error
    error     = null
  }

  ;++job.retries

  if (error) {
    job.addError(error)
  }
  job.addMessage('Retry set on "' + job.queue + ':' + job.parent.id + '".')

  job.forward('queued', callback)
  job.parent._done()

  return job
}

/**
 * Mark a job as done.
 *
 * @param {Error} error
 */
Job.prototype.done = function done (error, callback) {
  var job   = this
  if (job.status === 'timeout') return false

  var dest  = ''

  if (job._timer) clearTimeout(job._timer)

  if ('function' === typeof error) {
    callback  = error
    error     = null
  }

  // Move to inactive list
  if (error) {
    job.addError(error)
    dest    = 'failed'
  } else {
    job.addMessage('Done on "' + job.queue + ':' + job.parent.id + '".')
    dest    = 'completed'
  }

  job.forward(dest, callback)
  job.parent._done()

  return job
}
