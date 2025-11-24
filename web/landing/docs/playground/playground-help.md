# playground_help_controller.js

Display contextual help and documentation.

## Public Methods

### `show(event?: Event): void`
Shows the help section.

### `close(event?: Event): void`
Hides the help section.

### `showTopic(event: Event): void`
Shows specific help topic by ID.

**Usage:**
```html
<button data-action="click->playground-help#showTopic"
        data-help-topic="topic-id">
  Show Help
</button>
```

**Process:**
1. Gets topic ID from `event.currentTarget.dataset.helpTopic`
2. Hides all `.help-topic` elements
3. Shows help section
4. Shows topic element with matching ID

## Events Dispatched

None

## File: `playground_help_controller.js`
