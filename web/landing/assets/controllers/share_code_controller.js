import { Controller } from "@hotwired/stimulus"
import LZString from "lz-string"

export default class extends Controller {
    static outlets = ["code-editor"]

    codeEditorOutletConnected() {
        this.loadCodeFromUrl()
    }

    loadCodeFromUrl() {
        const query = new URLSearchParams(window.location.search)

        if (query.has('c')) {
            try {
                const compressed = query.get('c')
                console.log('[ShareCode] Compressed code:', compressed)

                const decompressed = LZString.decompressFromEncodedURIComponent(compressed)
                console.log('[ShareCode] Decompressed code:', decompressed)
                console.log('[ShareCode] Has code editor outlet:', this.hasCodeEditorOutlet)

                if (decompressed && this.hasCodeEditorOutlet) {
                    console.log('[ShareCode] Setting editor value...')
                    this.codeEditorOutlet.setValue(decompressed)
                } else if (!decompressed) {
                    console.error('[ShareCode] Decompression returned null or empty')
                }
            } catch (e) {
                console.error('Failed to decompress code:', e)
            }
        }
    }

    share() {
        if (!this.hasCodeEditorOutlet) {
            console.error('Code editor outlet not found')
            return
        }

        const code = this.codeEditorOutlet.getCode()
        console.log('[ShareCode] Original code to compress:', code)

        const url = new URL(window.location.href)
        url.search = ''

        const compressed = LZString.compressToEncodedURIComponent(code)
        console.log('[ShareCode] Compressed code:', compressed)

        url.searchParams.set('c', compressed)

        const link = url.toString()
        console.log('[ShareCode] Share link:', link)

        window.history.pushState({}, '', link)

        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(link).then(() => {
                alert('Share link copied to clipboard!')
            }).catch((err) => {
                console.error('Failed to copy:', err)
                prompt('Copy this link:', link)
            })
        } else {
            prompt('Copy this link:', link)
        }
    }
}
