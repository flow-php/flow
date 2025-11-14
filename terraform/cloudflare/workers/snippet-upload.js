export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url)
      const corsHeaders = getCorsHeaders(request)

      if (request.method === 'OPTIONS') {
        return handleCors(corsHeaders)
      }

      if (url.pathname === '/api/playground/snippets' && request.method === 'POST') {
        return await handleUpload(request, env, corsHeaders)
      }

      // Serve files from R2: /snippets/{id}/snippet.json, /snippets/{id}/code.php, /snippets/{id}/datasets/{name}
      if (url.pathname.startsWith('/snippets/') && request.method === 'GET') {
        return await handleGetFile(url.pathname, env, corsHeaders)
      }

      return jsonResponse({ success: false, error: 'Not found' }, 404, corsHeaders)
    } catch (error) {
      console.error('Worker error:', error)
      return jsonResponse(
        { success: false, error: 'Internal server error' },
        500,
        getCorsHeaders(request)
      )
    }
  }
}

/**
 * Handle file retrieval from R2 (GET /snippets/...)
 * @param {string} pathname - Request pathname
 * @param {object} env - Environment bindings (R2, KV, secrets)
 * @param {object} corsHeaders - CORS headers
 */
async function handleGetFile(pathname, env, corsHeaders) {
  try {
    // Remove leading slash: /snippets/abc/snippet.json -> snippets/abc/snippet.json
    const r2Key = pathname.substring(1)

    console.log(`[R2 Get] Fetching: ${r2Key}`)

    const object = await env.SNIPPETS_BUCKET.get(r2Key)

    if (!object) {
      console.warn(`[R2 Get] Not found: ${r2Key}`)
      return jsonResponse({ success: false, error: 'File not found' }, 404, corsHeaders)
    }

    console.log(`[R2 Get] Found: ${r2Key}`)

    // Determine content type
    let contentType = 'application/octet-stream'
    if (r2Key.endsWith('.json')) {
      contentType = 'application/json'
    } else if (r2Key.endsWith('.php')) {
      contentType = 'text/plain'
    } else if (r2Key.endsWith('.csv')) {
      contentType = 'text/csv'
    } else if (r2Key.endsWith('.xml')) {
      contentType = 'application/xml'
    }

    return new Response(object.body, {
      status: 200,
      headers: {
        'Content-Type': contentType,
        ...corsHeaders
      }
    })
  } catch (error) {
    console.error('[R2 Get] Error:', error)
    return jsonResponse({ success: false, error: 'Failed to retrieve file' }, 500, corsHeaders)
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
    // 1. Verify Turnstile token
    console.log('[Upload] Step 1: Verifying Turnstile')
    const turnstileResult = await verifyTurnstile(request, env.TURNSTILE_SECRET_KEY, env)
    if (!turnstileResult.success) {
      console.warn('[Upload] Turnstile verification failed')
      return jsonResponse({
        success: false,
        error: turnstileResult.error
      }, 403, corsHeaders)
    }

    // 2. Parse and validate upload
    console.log('[Upload] Step 2: Validating files')
    const ip = request.headers.get('CF-Connecting-IP') || 'unknown'
    const ipHash = await hashIP(ip)
    const formData = await request.formData()
    const validation = await validateUpload(formData)

    if (!validation.valid) {
      console.warn('[Upload] Validation failed:', validation.error)
      return jsonResponse({
        success: false,
        error: validation.error
      }, 400, corsHeaders)
    }

    // 3. Upload to R2
    console.log('[Upload] Step 3: Uploading to R2')
    const uploadResult = await uploadSnippetToR2(
      validation.files,
      ipHash,
      env.SNIPPETS_BUCKET
    )

    // 4. Return success response
    console.log('[Upload] Upload successful:', uploadResult.snippet_id)
    return jsonResponse({
      success: true,
      snippet_id: uploadResult.snippet_id,
      url: uploadResult.url,
      expires_at: uploadResult.expires_at
    }, 201, corsHeaders)

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
 * Inline nanoid implementation using Web Crypto API
 * Generates URL-safe random IDs (default 21 characters)
 * No external dependencies needed - uses Cloudflare Workers' crypto API
 */
const urlAlphabet = 'useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict'

const nanoid = (size = 21) => {
  let id = ''
  const bytes = crypto.getRandomValues(new Uint8Array(size))
  while (size--) {
    id += urlAlphabet[bytes[size] & 63]
  }
  return id
}

/**
 * Generate snippet ID using nanoid
 * @returns {string} - Snippet ID (21 character nanoid)
 */
function generateSnippetId() {
  return nanoid()
}

/**
 * Validate file type by extension
 * @param {string} filename - File name
 * @param {string} allowedType - 'code' or 'dataset'
 * @returns {object} - { valid: boolean, error?: string }
 */
function validateFileType(filename, allowedType) {
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
  const maxSize = fileType === 'code' ? 1048576 : 2097152 // 1MB or 2MB
  const maxSizeMB = fileType === 'code' ? '1MB' : '2MB'

  if (size > maxSize) {
    const sizeMB = (size / 1048576).toFixed(2)
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
  for (let i = 1; i <= 3; i++) {
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
  if (totalSize > 7340032) {
    const totalMB = (totalSize / 1048576).toFixed(2)
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
 * @param {object} validatedFiles - { code: File, datasets: File[] }
 * @param {string} ipHash - Hashed IP
 * @param {R2Bucket} bucket - R2 bucket binding
 * @returns {Promise<object>} - Snippet info
 */
async function uploadSnippetToR2(validatedFiles, ipHash, bucket) {
  // Generate ID
  const snippetId = generateSnippetId()
  const basePath = `snippets/${snippetId}`

  console.log(`[R2 Upload] Uploading to ${basePath}`)

  // Timestamps
  const createdAt = new Date().toISOString()
  const expiresAt = new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString()

  // Upload code file
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

  // Upload datasets to datasets/ subfolder
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

  // Create metadata
  const metadata = {
    snippet_id: snippetId,
    created_at: createdAt,
    expires_at: expiresAt,
    files: files,
    total_size: totalSize,
    ip_hash: ipHash
  }

  // Upload metadata as snippet.json
  await bucket.put(
    `${basePath}/snippet.json`,
    JSON.stringify(metadata, null, 2),
    {
      httpMetadata: {
        contentType: 'application/json'
      }
    }
  )

  console.log(`[R2 Upload] Successfully uploaded snippet ${snippetId}`)

  return {
    snippet_id: snippetId,
    url: `https://flow-php.com/playground?snippet=${snippetId}`,
    expires_at: expiresAt,
    files_uploaded: files.length
  }
}

// =============================================================================
// HTTP Response Helpers
// =============================================================================

/**
 * Get CORS headers
 * Allows both production (flow-php.com) and local development (flow-php.wip) domains
 */
function getCorsHeaders(request) {
  const origin = request?.headers?.get('Origin') || ''
  const allowedOrigins = ['https://flow-php.com', 'https://www.flow-php.com', 'https://flow-php.wip']

  const allowOrigin = allowedOrigins.includes(origin) ? origin : 'https://flow-php.com'

  return {
    'Access-Control-Allow-Origin': allowOrigin,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, CF-Turnstile-Response',
    'Access-Control-Max-Age': '86400',
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
