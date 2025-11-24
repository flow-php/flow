# playground_reset_controller.js

Reset playground to default state.

## Outlets
- `playground-storage` - Storage controller

## Public Methods

### `confirmReset(event: Event): void`
Shows confirmation dialog and clears storage if confirmed.

**Process:**
1. Shows browser confirm dialog
2. If canceled, prevents default action (page reload)
3. If confirmed, clears code from localStorage via storage outlet
4. Allows default action (page reload via link href)

## Events Dispatched

None (delegates to `playground-storage` outlet)

## File: `playground_reset_controller.js`
