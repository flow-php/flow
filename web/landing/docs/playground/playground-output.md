# playground_output_controller.js

Display execution output and messages with timestamps and type indicators.

## Public Methods

### `show(message: Object): void`
Replaces output container content with new message.

**Message format:**
```javascript
{
  content: string,        // Message text
  type: string,          // Message type (see below)
  color?: string,        // Optional custom color
  timestamp?: number     // Optional timestamp (defaults to now)
}
```

**Message types:**
- `info` - Info messages (prefix: `[INFO]`)
- `success` - Success messages (prefix: `[SUCCESS]`)
- `warning` - Warning messages (prefix: `[WARNING]`)
- `error` - Error messages (prefix: `[ERROR]`)

### `append(message: Object): void`
Appends message to existing output (adds newline separator).

### `clear(): void`
Clears all output.

### `isLoaded(): boolean`
Returns true (always loaded).

### `onLoad(): Promise<void>`
Returns resolved promise (always ready).

## Events Dispatched

### `playground-output:shown`
**Trigger:** New message displayed (replaces content)
**Payload:** `{ message: string, type: string }`
**Bubbles:** Yes

### `playground-output:appended`
**Trigger:** Message appended to existing output
**Payload:** `{ message: string, type: string }`
**Bubbles:** Yes

### `playground-output:cleared`
**Trigger:** Output cleared
**Payload:** `{ timestamp: number }`
**Bubbles:** Yes

## Output Format

Each message rendered as:
```html
<div class="output-message output-{type}">
  <span class="output-timestamp">HH:MM:SS</span>
  <span class="output-prefix">[TYPE]</span>
  <span class="output-content">escaped message text</span>
</div>
```

## File: `playground_output_controller.js`
