/**
 * Requires.
 */
var redis  = require('node-redis')
  , Filter = require('filter')
  , uuid   = require('node-uuid')
  , util   = require('util')

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
 * Handle an error appropiatly.
 *
 * @param {Error} error: The error object in question.
 * @param {Function} callback: Optional callback to pass error to.
 */
var handleError = function (error, callback) {
  if (callback) return callback(error)
  throw error
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
  queue.onPop       = function onPop (error) {
    if (error) return queue.emit('error', error)
    queue._next()

    queue.client.watch(queue._prefix + ':queued')
    queue.client.zrevrange(queue._prefix + ':queued', '0', '0', gotQueued)
  }

  // On zrevrange
  function gotQueued (error, job_id) {
    if (error) return queue.emit('error', error)
    if (!job_id || !job_id[0]) return

    queue._processJob(job_id[0].toString())
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
 * Adds a new job to the queue.
 *
 * @param {Object} payload: The data payload to enqueue.
 * @param {Function} callback
 */
Queue.prototype.write = function write (payload, callback) {
  var queue       = this
  var now         = Date.now()
  var job         = new Job(
    queue
  , { id          : uuid()
    , status      : 'queued'
    , priority    : 1
    , timeout     : queue.timeout
    , retries     : 0
    , msg_count   : 0
    , msgs        : []
    , payload     : payload
    , created     : now
    , updated     : now
    }
  , true
  )

  // Push the job.
  job.forward('queued', callback)

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
    return callback(error)
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

  if (queue.paused) return

  if (queue.processing >= queue.concurrency) {
    return queue
  }

  ;++queue.processing

  queue.bclient.blpop(queue._prefix + ':listen', queue.onPop)

  return queue
}

/**
 * Process a job. At this stage watch has been called on the queued sorted set
 * and we have a job id.
 */
Queue.prototype._processJob = function processJob (job_id) {
  var queue   = this
  var client  = queue.client
  var status  = 'running:' + queue.id
  var now     = Date.now()

  client.multi()
  client.hget(queue._prefix + ':jobs', job_id)
  client.zrem(queue._prefix + ':queued', job_id)
  client.zadd(queue._prefix + ':' + status, now, job_id)

  client.exec(function (error, results) {
    if (error) return queue.emit('error', error)
    if (results === null) {
      client.watch(queue._prefix + ':queued')
      return queue._processJob(job_id)
    }

    queue.emit('data', queue._returnJob(results[0]))
  })
}

/**
 * Start the queue listener
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
      if (error) return self.emit('error', error)
      if (callback) callback()
      queue._next()
    }
  )

  return queue
}

/**
 * Stop yol horses. Shut her down!
 */
Queue.prototype.end = function end () {
  var queue     = this

  if (queue.bclient) {
    queue.bclient.destroy()
    queue.bclient = null
  }
  if (queue.sclient) {
    queue.sclient.destroy()
    queue.sclient = null
  }
  queue.client.end()

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
  job.timeout     = data.timeout
  job.payload     = data.payload
  job.retries     = data.retries
  job.msg_count   = data.msg_count
  job.msgs        = data.msgs
  job.created     = data.created || Date.now()
  job.updated     = data.updated

  job.is_new      = is_new || false
  job.old_status  = job.status

  job.prefix      = parent.prefix
  job._prefix     = parent._prefix
  job.queue       = parent.name

  // Associate with a redis client.
  job.parent      = parent
  job.client      = parent.client
}

exports.Job = Job

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
    , payload   : job.payload
    , retries   : job.retries
    , msg_count : job.msg_count
    , msgs      : job.msgs
    , created   : job.created
    , updated   : job.updated
    }
  )
}

/**
 * Save a job.
 *
 * @param {Function} callback
 */
Job.prototype.save = function save (callback) {
  var job     = this

  job.updated = Date.now()

  // We won't do a multi here, as usually it has been called elsewhere.
  job.client.hset(
    self.prefix + 'rq:' + self.queue + ':jobs'
  , job.id
  , JSON.stringify(job)
  , callback
  )

  job.client.publish(self.prefix + 'rq:' + job.queue + ':save' , job.id)

  return job
}

/**
 * Forward job to another status set.
 *
 * @param {String} dest
 * @param {Function} callback
 */
Job.prototype.forward = function forward (dest, callback) {
  var job     = this
  var client  = job.client
  var score   = 0

  job.status  = dest

  if (!callback) {
    callback  = job.parent._onError
  }

  if (job.is_new) {
    client.multi()
  } else {
    client.watch(job._prefix + ':' + job.old_status)
    client.multi()
    client.zrem(job._prefix + ':' + job.old_status, job.id)
  }

  job.save()
  score       = job.updated

  // Queued items need to be listened for, and sorted set score assigned the
  // job priority.
  if ('queued' === job.status) {
    score     = job.priority
    client.rpush(job._prefix + ':listen', '1')
  }

  client.zadd(
    job._prefix ':' + job.status
  , score
  , job.id
  )
  client.publish(job._prefix + ':' + job.status, job.id)

  function onExec (error, data) {
    if (error) return callback(error)
    if (null === data) return job.forward(dest, callback)
    callback(null, job)
  }

  client.exec(onExec)

  return job
}

/**
 * Re-process the job by adding back to the queue.
 *
 * @param {Function} callback: The optional callback
 */
Job.prototype.retry = function (error, callback) {
  var job = this
  if (job.timeout) cleartimeout(job.timeout)

  if ('function' === typeof error) {
    callback  = error
    error     = null
  }

  ;++job.retries

  if (error) {
    job.addError(error)
  }
  job.addMessage('Retrying on "' + job.queue + ':' + job.parent.id + '".')

  job.forward('queued', callback)

  return job
}

/**
 * Mark a job as done.
 *
 * @param {Error} error
 */
Job.prototype.done = function done (error) {
  if (job.status === 'timeout') return

  var job   = this
  var dest  = ''

  if (job.timeout) cleartimeout(job.timeout)

  // Move to inactive list
  if (error) {
    job.addError(error)
    dest    = 'failed'
  } else {
    job.addMessage('Done on "' + job.queue + ':' + job.parent.id + '".')
    dest    = 'completed'
  }

  job.forward(dest, callback)

  return job
}
