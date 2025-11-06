import Violation from './Violation.js'

export default class Violations {
    #violations

    constructor(violations = []) {
        this.#violations = violations
    }

    static fromJson(jsonString) {
        try {
            const parsed = jsonString ? JSON.parse(jsonString) : []
            if (!Array.isArray(parsed)) {
                return new Violations([])
            }
            const violations = parsed.map(violationData => Violation.create(violationData))
            return new Violations(violations)
        } catch (error) {
            console.error('Failed to parse violations JSON:', error)
            return new Violations([])
        }
    }

    get violations() {
        return [...this.#violations]
    }

    get length() {
        return this.#violations.length
    }
}
