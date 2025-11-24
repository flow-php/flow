// =============================================================================
// Constants
// =============================================================================

const RATE_LIMITS = {
  HOURLY_PER_IP: 30,
  DAILY_PER_IP: 100,
  DAILY_GLOBAL: 3500,
  HOUR_MS: 3600000,
  DAY_MS: 86400000
}

const FILE_LIMITS = {
  CODE_MAX_SIZE: 1048576,      // 1MB
  DATASET_MAX_SIZE: 2097152,   // 2MB
  TOTAL_MAX_SIZE: 7340032,     // 7MB
  MAX_DATASETS: 3,
  SNIPPET_EXPIRY_DAYS: 90
}

const REQUEST_LIMITS = {
  MAX_BODY_SIZE: 10485760      // 10MB
}

/**
 * Durable Object for rate limiting snippet uploads
 *
 * Rate limits:
 * - 30 uploads per hour per IP
 * - 100 uploads per day per IP
 * - 3,500 uploads per day globally
 */
export class SnippetRateLimiter {
  #state
  #env

  constructor(state, env) {
    this.#state = state
    this.#env = env
  }

  async fetch(request) {
    try {
      const url = new URL(request.url)

      if (url.pathname === '/check' && request.method === 'POST') {
        return await this.#checkRateLimit(request)
      }

      return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' }
      })
    } catch (error) {
      console.error('[RateLimiter] Error:', error)
      return new Response(JSON.stringify({
        allowed: false,
        error: 'Internal error'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      })
    }
  }

  async #checkRateLimit(request) {
    const { ipHash } = await request.json()

    if (!ipHash) {
      return this.#jsonResponse({ allowed: false, error: 'IP hash required' }, 400)
    }

    await this.#ensureSchema()

    const now = Date.now()
    const hourAgo = now - RATE_LIMITS.HOUR_MS
    const dayAgo = now - RATE_LIMITS.DAY_MS

    const hourlyUploads = await this.#getUploadCount(ipHash, hourAgo)
    const dailyUploads = await this.#getUploadCount(ipHash, dayAgo)
    const globalDailyUploads = await this.#getGlobalUploadCount(dayAgo)

    if (hourlyUploads >= RATE_LIMITS.HOURLY_PER_IP) {
      return this.#jsonResponse({
        allowed: false,
        error: `Rate limit exceeded: maximum ${RATE_LIMITS.HOURLY_PER_IP} uploads per hour`,
        retry_after: await this.#getRetryAfter(ipHash, RATE_LIMITS.HOUR_MS)
      }, 429)
    }

    if (dailyUploads >= RATE_LIMITS.DAILY_PER_IP) {
      return this.#jsonResponse({
        allowed: false,
        error: `Rate limit exceeded: maximum ${RATE_LIMITS.DAILY_PER_IP} uploads per day`,
        retry_after: await this.#getRetryAfter(ipHash, RATE_LIMITS.DAY_MS)
      }, 429)
    }

    if (globalDailyUploads >= RATE_LIMITS.DAILY_GLOBAL) {
      return this.#jsonResponse({
        allowed: false,
        error: 'Service temporarily unavailable: daily upload quota reached',
        retry_after: this.#getNextDayReset()
      }, 503)
    }

    await this.#recordUpload(ipHash, now)

    return this.#jsonResponse({
      allowed: true,
      remaining: {
        hourly: RATE_LIMITS.HOURLY_PER_IP - hourlyUploads - 1,
        daily: RATE_LIMITS.DAILY_PER_IP - dailyUploads - 1,
        global: RATE_LIMITS.DAILY_GLOBAL - globalDailyUploads - 1
      }
    })
  }

  async #getUploadCount(ipHash, sinceTimestamp) {
    const sql = this.#state.storage.sql
    const result = await sql.exec(
      `SELECT COUNT(*) as count FROM uploads
       WHERE ip_hash = ? AND timestamp > ?`,
      ipHash,
      sinceTimestamp
    )

    return result.toArray()[0]?.count || 0
  }

  async #getGlobalUploadCount(sinceTimestamp) {
    const sql = this.#state.storage.sql
    const result = await sql.exec(
      `SELECT COUNT(*) as count FROM uploads
       WHERE timestamp > ?`,
      sinceTimestamp
    )

    return result.toArray()[0]?.count || 0
  }

  async #ensureSchema() {
    const sql = this.#state.storage.sql

    await sql.exec(
      `CREATE TABLE IF NOT EXISTS uploads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ip_hash TEXT NOT NULL,
        timestamp INTEGER NOT NULL
      )`
    )

    await sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_ip_timestamp
       ON uploads(ip_hash, timestamp)`
    )

    await sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_timestamp
       ON uploads(timestamp)`
    )
  }

  async #recordUpload(ipHash, timestamp) {
    const sql = this.#state.storage.sql

    await sql.exec(
      `INSERT INTO uploads (ip_hash, timestamp) VALUES (?, ?)`,
      ipHash,
      timestamp
    )

    await this.#cleanupOldRecords(timestamp - RATE_LIMITS.DAY_MS)
  }

  async #cleanupOldRecords(beforeTimestamp) {
    const sql = this.#state.storage.sql
    await sql.exec(
      `DELETE FROM uploads WHERE timestamp < ?`,
      beforeTimestamp
    )
  }

  async #getRetryAfter(ipHash, windowMs) {
    const sql = this.#state.storage.sql
    const limit = windowMs === RATE_LIMITS.HOUR_MS ? RATE_LIMITS.HOURLY_PER_IP : RATE_LIMITS.DAILY_PER_IP
    const result = await sql.exec(
      `SELECT MIN(timestamp) as oldest FROM uploads
       WHERE ip_hash = ?
       ORDER BY timestamp DESC
       LIMIT ?`,
      ipHash,
      limit
    )

    const oldest = result.toArray()[0]?.oldest
    if (!oldest) {
      return 60
    }

    const retryAfter = Math.ceil((oldest + windowMs - Date.now()) / 1000)
    return Math.max(retryAfter, 1)
  }

  #getNextDayReset() {
    const now = new Date()
    const tomorrow = new Date(now)
    tomorrow.setUTCDate(tomorrow.getUTCDate() + 1)
    tomorrow.setUTCHours(0, 0, 0, 0)

    return Math.ceil((tomorrow - now) / 1000)
  }

  #jsonResponse(data, status = 200) {
    return new Response(JSON.stringify(data, null, 2), {
      status,
      headers: { 'Content-Type': 'application/json' }
    })
  }
}

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url)
      const corsHeaders = getCorsHeaders(request, env)

      if (request.method === 'OPTIONS') {
        return handleCors(corsHeaders)
      }

      if (url.pathname === '/api/playground/snippets' && request.method === 'POST') {
        return await handleUpload(request, env, corsHeaders)
      }

      if (url.pathname.startsWith('/snippets/') && request.method === 'GET') {
        // Only serve files via worker in local dev (for testing with emulated R2)
        // In production, files are served directly from public R2 domain
        if (env.ENABLE_R2_PROXY === 'true') {
          return await handleGetSnippet(url.pathname, env, corsHeaders)
        }
        return jsonResponse({
          success: false,
          error: 'Files are served from public R2 domain in production'
        }, 404, corsHeaders)
      }

      return jsonResponse({ success: false, error: 'Not found' }, 404, corsHeaders)
    } catch (error) {
      console.error('Worker error:', error)
      return jsonResponse(
        { success: false, error: 'Internal server error' },
        500,
        getCorsHeaders(request, env)
      )
    }
  }
}

/**
 * Handle snippet upload (POST /api/playground/snippets)
 * @param {Request} request - The request object
 * @param {object} env - Environment bindings (R2, KV, secrets)
 * @param {object} corsHeaders - CORS headers
 */
async function handleUpload(request, env, corsHeaders) {
  console.log('[Upload] Request received')

  try {
    // 1. Check rate limits (Durable Object)
    console.log('[Upload] Step 1: Checking rate limits')
    const ip = request.headers.get('CF-Connecting-IP') || 'unknown'
    const ipHash = await hashIP(ip)

    const rateLimitResult = await checkRateLimit(ipHash, env.RATE_LIMITER, env)
    if (!rateLimitResult.allowed) {
      console.warn('[Upload] Rate limit exceeded for IP hash:', ipHash.substring(0, 8))
      return jsonResponse({
        success: false,
        error: rateLimitResult.error,
        retry_after: rateLimitResult.retry_after
      }, rateLimitResult.status || 429, corsHeaders)
    }

    // 2. Verify Turnstile token
    console.log('[Upload] Step 2: Verifying Turnstile')
    const turnstileResult = await verifyTurnstile(request, env.TURNSTILE_SECRET_KEY, env)
    if (!turnstileResult.success) {
      console.warn('[Upload] Turnstile verification failed')
      return jsonResponse({
        success: false,
        error: turnstileResult.error
      }, 403, corsHeaders)
    }

    // 3. Parse and validate upload
    console.log('[Upload] Step 3: Validating files')
    const formData = await request.formData()

    const snippetId = formData.get('snippet_id')
    if (!snippetId) {
      return jsonResponse({
        success: false,
        error: 'snippet_id is required'
      }, 400, corsHeaders)
    }

    const snippetIdValidation = validateSnippetId(snippetId)
    if (!snippetIdValidation.valid) {
      return jsonResponse({
        success: false,
        error: snippetIdValidation.error
      }, 400, corsHeaders)
    }

    const validation = await validateUpload(formData)

    if (!validation.valid) {
      console.warn('[Upload] Validation failed:', validation.error)
      return jsonResponse({
        success: false,
        error: validation.error
      }, 400, corsHeaders)
    }

    // 4. Upload to R2
    console.log('[Upload] Step 4: Uploading to R2 with snippet ID:', snippetId)
    const uploadResult = await uploadSnippetToR2(
      snippetId,
      validation.files,
      ipHash,
      env.SNIPPETS_BUCKET
    )

    // 5. Return success response
    console.log('[Upload] Upload successful:', uploadResult.snippet_id, 'message:', uploadResult.message)
    return jsonResponse({
      success: true,
      snippet_id: uploadResult.snippet_id,
      message: uploadResult.message,
      url: uploadResult.url,
      expires_at: uploadResult.expires_at,
      created_at: uploadResult.created_at
    }, uploadResult.message === 'exists' ? 200 : 201, corsHeaders)

  } catch (error) {
    console.error('[Upload] Error:', error)
    return jsonResponse({
      success: false,
      error: 'Upload failed: ' + error.message
    }, 500, corsHeaders)
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Check rate limit using Durable Object
 * @param {string} ipHash - Hashed IP address
 * @param {DurableObjectNamespace} rateLimiterNamespace - Rate limiter namespace
 * @param {object} env - Environment bindings (for checking RATE_LIMITER_MODE)
 * @returns {Promise<object>} - { allowed: boolean, error?: string, retry_after?: number, status?: number }
 */
async function checkRateLimit(ipHash, rateLimiterNamespace, env) {
  const mode = env?.RATE_LIMITER_MODE || 'enforce'

  // Mode: bypass - completely skip rate limiting (for testing)
  if (mode === 'bypass') {
    console.log('[RateLimit] Mode: bypass - skipping rate limit check')
    return { allowed: true }
  }

  // Mode: enforce - full production rate limiting
  try {
    const id = rateLimiterNamespace.idFromName('rate-limiter')
    const stub = rateLimiterNamespace.get(id)

    const response = await stub.fetch('https://rate-limiter/check', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ipHash })
    })

    const result = await response.json()

    if (!response.ok) {
      return {
        allowed: false,
        error: result.error || 'Rate limit exceeded',
        retry_after: result.retry_after,
        status: response.status
      }
    }

    return result
  } catch (error) {
    console.error('[RateLimit] Error checking rate limit:', error)
    // Fail closed for security - deny on error
    return {
      allowed: false,
      error: 'Rate limiting service temporarily unavailable',
      status: 503
    }
  }
}

/**
 * Verify Turnstile CAPTCHA token
 * @param {Request} request - The incoming request
 * @param {string} secretKey - Turnstile secret key from env
 * @param {object} env - Environment bindings (for checking TURNSTILE_MODE)
 * @returns {Promise<object>} - { success: boolean, error?: string }
 */
async function verifyTurnstile(request, secretKey, env) {
  const mode = env.TURNSTILE_MODE || 'verify'

  // Mode: bypass - completely skip verification (no token required)
  if (mode === 'bypass') {
    console.log('[Turnstile] Mode: bypass - skipping verification')
    return { success: true }
  }

  // Extract token from header
  const token = request.headers.get('CF-Turnstile-Response')

  if (!token) {
    console.warn('[Turnstile] No token provided')
    return { success: false, error: 'CAPTCHA token is required' }
  }

  // Mode: mock - accept any token without calling Cloudflare API (for testing widget flow)
  if (mode === 'mock') {
    console.log('[Turnstile] Mode: mock - accepting token without verification')
    return { success: true }
  }

  // Mode: mock-fail - reject any token (for testing error handling)
  if (mode === 'mock-fail') {
    console.log('[Turnstile] Mode: mock-fail - rejecting token for testing')
    return { success: false, error: 'CAPTCHA verification failed (mock failure)' }
  }

  // Mode: verify - full production verification with Cloudflare API
  console.log('[Turnstile] Mode: verify - calling Cloudflare API')

  // Get client IP
  const ip = request.headers.get('CF-Connecting-IP') || ''

  // Prepare verification request
  const formData = new FormData()
  formData.append('secret', secretKey)
  formData.append('response', token)
  formData.append('remoteip', ip)

  try {
    // Call Cloudflare Turnstile API
    const verifyResponse = await fetch(
      'https://challenges.cloudflare.com/turnstile/v0/siteverify',
      {
        method: 'POST',
        body: formData,
      }
    )

    const outcome = await verifyResponse.json()

    if (!outcome.success) {
      console.warn('[Turnstile] Verification failed:', outcome['error-codes'])
      return {
        success: false,
        error: 'CAPTCHA verification failed'
      }
    }

    console.log('[Turnstile] Verification successful')
    return { success: true }

  } catch (error) {
    console.error('[Turnstile] Verification error:', error)
    return {
      success: false,
      error: 'CAPTCHA verification error'
    }
  }
}

/**
 * Validate snippet ID (SHA-256 fingerprint)
 * @param {string} snippetId - Snippet ID
 * @returns {object} - { valid: boolean, error?: string }
 */
function validateSnippetId(snippetId) {
  if (!snippetId || typeof snippetId !== 'string') {
    return { valid: false, error: 'Snippet ID is required' }
  }

  if (snippetId.length !== 64) {
    return { valid: false, error: 'Snippet ID must be 64 characters (SHA-256 hex)' }
  }

  const hexPattern = /^[0-9a-f]{64}$/
  if (!hexPattern.test(snippetId)) {
    return { valid: false, error: 'Snippet ID must be a valid SHA-256 hex string (lowercase)' }
  }

  return { valid: true }
}

/**
 * Validate filename for security (no sanitization - reject invalid names)
 * @param {string} filename - File name
 * @returns {object} - { valid: boolean, error?: string }
 */
function validateFilename(filename) {
  if (!filename || filename.length === 0) {
    return { valid: false, error: 'Filename cannot be empty' }
  }

  if (filename.length > 255) {
    return { valid: false, error: 'Filename too long (max 255 characters)' }
  }

  // Check for path traversal attempts
  if (filename.includes('..') || filename.includes('/') || filename.includes('\\')) {
    return { valid: false, error: 'Filename cannot contain path separators or parent directory references' }
  }

  // Only allow safe characters: alphanumeric, dash, underscore, dot, parentheses, spaces
  const safePattern = /^[a-zA-Z0-9._\-() ]+$/
  if (!safePattern.test(filename)) {
    return { valid: false, error: 'Filename contains invalid characters. Only letters, numbers, spaces, dash, underscore, dot, and parentheses are allowed' }
  }

  // Must have an extension
  if (!filename.includes('.')) {
    return { valid: false, error: 'Filename must have an extension' }
  }

  return { valid: true }
}

/**
 * Validate file type by extension
 * @param {string} filename - File name
 * @param {string} allowedType - 'code' or 'dataset'
 * @returns {object} - { valid: boolean, error?: string }
 */
function validateFileType(filename, allowedType) {
  // Validate filename first
  const filenameCheck = validateFilename(filename)
  if (!filenameCheck.valid) {
    return filenameCheck
  }

  const ext = filename.split('.').pop().toLowerCase()

  if (allowedType === 'code') {
    if (ext !== 'php') {
      return { valid: false, error: `Code file must be .php, got .${ext}` }
    }
  } else if (allowedType === 'dataset') {
    if (!['csv', 'json', 'xml'].includes(ext)) {
      return { valid: false, error: `Dataset must be .csv, .json, or .xml, got .${ext}` }
    }
  }

  return { valid: true }
}

/**
 * Validate file size
 * @param {number} size - File size in bytes
 * @param {string} fileType - 'code' or 'dataset'
 * @returns {object} - { valid: boolean, error?: string }
 */
function validateFileSize(size, fileType) {
  const maxSize = fileType === 'code' ? FILE_LIMITS.CODE_MAX_SIZE : FILE_LIMITS.DATASET_MAX_SIZE
  const maxSizeMB = fileType === 'code' ? '1MB' : '2MB'

  if (size > maxSize) {
    const sizeMB = (size / FILE_LIMITS.CODE_MAX_SIZE).toFixed(2)
    return {
      valid: false,
      error: `${fileType} file too large: ${sizeMB}MB (max ${maxSizeMB})`
    }
  }

  return { valid: true }
}

/**
 * Validate upload structure
 * @param {FormData} formData - The multipart form data
 * @returns {object} - { valid: boolean, error?: string, files?: object }
 */
async function validateUpload(formData) {
  const codeFile = formData.get('code')
  const datasets = []
  let totalSize = 0

  // Check code file exists
  if (!codeFile) {
    return { valid: false, error: 'Code file is required' }
  }

  // Validate code file
  const codeTypeCheck = validateFileType(codeFile.name, 'code')
  if (!codeTypeCheck.valid) {
    return codeTypeCheck
  }

  const codeSizeCheck = validateFileSize(codeFile.size, 'code')
  if (!codeSizeCheck.valid) {
    return codeSizeCheck
  }

  totalSize += codeFile.size

  // Check datasets
  for (let i = 1; i <= FILE_LIMITS.MAX_DATASETS; i++) {
    const dataset = formData.get(`dataset_${i}`)
    if (dataset) {
      // Validate type
      const typeCheck = validateFileType(dataset.name, 'dataset')
      if (!typeCheck.valid) {
        return typeCheck
      }

      // Validate size
      const sizeCheck = validateFileSize(dataset.size, 'dataset')
      if (!sizeCheck.valid) {
        return sizeCheck
      }

      totalSize += dataset.size
      datasets.push(dataset)
    }
  }

  // Validate total size (7MB)
  if (totalSize > FILE_LIMITS.TOTAL_MAX_SIZE) {
    const totalMB = (totalSize / FILE_LIMITS.CODE_MAX_SIZE).toFixed(2)
    return {
      valid: false,
      error: `Total upload size ${totalMB}MB exceeds 7MB limit`
    }
  }

  return {
    valid: true,
    files: {
      code: codeFile,
      datasets: datasets
    },
    totalSize: totalSize
  }
}

/**
 * Hash IP address using SHA-256
 * @param {string} ip - IP address
 * @returns {Promise<string>} - Hex hash string
 */
async function hashIP(ip) {
  if (!ip) {
    return 'unknown'
  }

  const encoder = new TextEncoder()
  const data = encoder.encode(ip)
  const hashBuffer = await crypto.subtle.digest('SHA-256', data)
  const hashArray = Array.from(new Uint8Array(hashBuffer))
  const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('')

  return hashHex
}

/**
 * Upload snippet to R2
 * @param {string} snippetId - Client-provided snippet ID (SHA-256 fingerprint)
 * @param {object} validatedFiles - { code: File, datasets: File[] }
 * @param {string} ipHash - Hashed IP
 * @param {R2Bucket} bucket - R2 bucket binding
 * @returns {Promise<object>} - Snippet info
 */
async function uploadSnippetToR2(snippetId, validatedFiles, ipHash, bucket) {
  const basePath = `snippets/${snippetId}`
  const metadataKey = `${basePath}/snippet.json`

  console.log(`[R2 Upload] Checking if snippet exists: ${snippetId}`)

  const existingMetadata = await bucket.head(metadataKey)

  if (existingMetadata) {
    console.log(`[R2 Upload] Snippet already exists: ${snippetId}`)

    const existingObject = await bucket.get(metadataKey)
    const existingData = await existingObject.json()

    return {
      snippet_id: snippetId,
      message: 'exists',
      url: `https://flow-php.com/playground?snippet=${snippetId}`,
      expires_at: existingData.expires_at,
      created_at: existingData.created_at,
      files_uploaded: existingData.files.length
    }
  }

  console.log(`[R2 Upload] Uploading new snippet to ${basePath}`)

  const createdAt = new Date().toISOString()
  const expiresAt = new Date(Date.now() + FILE_LIMITS.SNIPPET_EXPIRY_DAYS * 24 * 60 * 60 * 1000).toISOString()

  const codeStream = validatedFiles.code.stream()
  await bucket.put(`${basePath}/code.php`, codeStream, {
    httpMetadata: {
      contentType: 'text/plain'
    }
  })

  const files = [{
    name: 'code.php',
    size: validatedFiles.code.size,
    type: 'code'
  }]

  let totalSize = validatedFiles.code.size

  for (const dataset of validatedFiles.datasets) {
    const datasetStream = dataset.stream()
    await bucket.put(`${basePath}/datasets/${dataset.name}`, datasetStream, {
      httpMetadata: {
        contentType: dataset.type || 'application/octet-stream'
      }
    })

    files.push({
      name: dataset.name,
      size: dataset.size,
      type: 'dataset'
    })

    totalSize += dataset.size
  }

  const metadata = {
    snippet_id: snippetId,
    created_at: createdAt,
    expires_at: expiresAt,
    files: files,
    total_size: totalSize,
    ip_hash: ipHash
  }

  await bucket.put(
    metadataKey,
    JSON.stringify(metadata, null, 2),
    {
      httpMetadata: {
        contentType: 'application/json'
      }
    }
  )

  console.log(`[R2 Upload] Successfully uploaded new snippet ${snippetId}`)

  return {
    snippet_id: snippetId,
    message: 'created',
    url: `https://flow-php.com/playground?snippet=${snippetId}`,
    expires_at: expiresAt,
    created_at: createdAt,
    files_uploaded: files.length
  }
}

/**
 * Handle GET request to serve snippet files from R2
 * For local development only - production uses public R2 URLs
 * @param {string} pathname - URL pathname like /snippets/{id}/snippet.json
 * @param {object} env - Environment bindings
 * @param {object} corsHeaders - CORS headers
 */
async function handleGetSnippet(pathname, env, corsHeaders) {
  try {
    // Parse path: /snippets/{id}/{file}
    const pathParts = pathname.split('/').filter(p => p)

    if (pathParts.length < 3 || pathParts[0] !== 'snippets') {
      return jsonResponse({ error: 'Invalid path' }, 404, corsHeaders)
    }

    const snippetId = pathParts[1]
    const fileName = pathParts.slice(2).join('/')

    // Construct R2 key
    const r2Key = `snippets/${snippetId}/${fileName}`

    // Fetch from R2
    const object = await env.SNIPPETS_BUCKET.get(r2Key)

    if (!object) {
      return jsonResponse({ error: 'File not found' }, 404, corsHeaders)
    }

    // Determine content type
    let contentType = 'application/octet-stream'
    if (fileName.endsWith('.json')) {
      contentType = 'application/json'
    } else if (fileName.endsWith('.php')) {
      contentType = 'text/plain; charset=utf-8'
    } else if (fileName.endsWith('.csv')) {
      contentType = 'text/csv'
    } else if (fileName.endsWith('.xml')) {
      contentType = 'application/xml'
    }

    return new Response(object.body, {
      headers: {
        'Content-Type': contentType,
        'Cache-Control': 'public, max-age=86400, immutable',
        'X-Content-Type-Options': 'nosniff',
        ...corsHeaders
      }
    })
  } catch (error) {
    console.error('[GetSnippet] Error:', error)
    return jsonResponse({ error: 'Internal error' }, 500, corsHeaders)
  }
}

// =============================================================================
// HTTP Response Helpers
// =============================================================================

/**
 * Get CORS headers
 * Allows both production (flow-php.com) and local development (flow-php.wip) domains
 * In bypass mode (for testing), allows localhost origins
 */
function getCorsHeaders(request, env) {
  // In bypass/mock mode, allow any origin for testing
  if (env?.TURNSTILE_MODE === 'bypass' || env?.TURNSTILE_MODE === 'mock') {
    const origin = request?.headers?.get('Origin') || '*'
    return {
      'Access-Control-Allow-Origin': origin,
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, CF-Turnstile-Response',
      'Access-Control-Max-Age': '86400'
    }
  }

  const origin = request?.headers?.get('Origin') || ''

  // Define allowed origins
  const allowedOrigins = [
    'https://flow-php.com',
    'https://www.flow-php.com',
    'https://flow-php.wip'
  ]

  const allowOrigin = allowedOrigins.includes(origin) ? origin : 'https://flow-php.com'

  return {
    'Access-Control-Allow-Origin': allowOrigin,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, CF-Turnstile-Response',
    'Access-Control-Max-Age': '86400',
    'Vary': 'Origin'
  }
}

/**
 * Handle CORS preflight requests
 */
function handleCors(corsHeaders) {
  return new Response(null, {
    status: 204,
    headers: corsHeaders
  })
}

/**
 * Create JSON response with CORS headers
 */
function jsonResponse(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...extraHeaders
    }
  })
}
