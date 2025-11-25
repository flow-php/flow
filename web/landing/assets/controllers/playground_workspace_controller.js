import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static outlets = ["wasm", "code-editor", "playground"]
    static targets = ["tree", "emptyState"]
    static values = {
        rootPath: { type: String, default: '/workspace' },
        folderIcon: String,
        fileIcon: String
    }

    #selectedFile = null
    #treeRendered = false
    #boundRefreshTree = null

    connect() {
        this.#boundRefreshTree = () => this.refreshTree()

        this.element.addEventListener('wasm:file-created', this.#boundRefreshTree)
        this.element.addEventListener('wasm:file-deleted', this.#boundRefreshTree)
        this.element.addEventListener('wasm:file-updated', this.#boundRefreshTree)
        this.element.addEventListener('playground-upload:file-uploaded', this.#boundRefreshTree)
        this.element.addEventListener('playground-upload:files-cleared', this.#boundRefreshTree)
    }

    disconnect() {
        if (this.#boundRefreshTree) {
            this.element.removeEventListener('wasm:file-created', this.#boundRefreshTree)
            this.element.removeEventListener('wasm:file-deleted', this.#boundRefreshTree)
            this.element.removeEventListener('wasm:file-updated', this.#boundRefreshTree)
            this.element.removeEventListener('playground-upload:file-uploaded', this.#boundRefreshTree)
            this.element.removeEventListener('playground-upload:files-cleared', this.#boundRefreshTree)
        }
    }

    onActionStarted() {
        if (this.hasTreeTarget) {
            this.treeTarget.classList.add('disabled')
        }
    }

    onActionFinished() {
        if (this.hasTreeTarget) {
            this.treeTarget.classList.remove('disabled')
        }
    }

    async refreshTree() {
        await this.playgroundOutlet.onLoad()

        if (!this.hasWasmOutlet) {
            return
        }

        const result = await this.wasmOutlet.listFiles(this.rootPathValue, true)

        if (!result.success) {
            this.#showEmptyState()
            return
        }

        const files = result.files || []
        const workspaceFiles = files
            .filter(file => file.path.startsWith(this.rootPathValue))
            .map(file => ({
                ...file,
                path: file.path.substring(this.rootPathValue.length) || '/'
            }))

        const tree = this.#buildFileTree(workspaceFiles)
        this.#renderTree(tree)

        const fileCount = workspaceFiles.filter(f => f.type === 'file').length

        this.dispatch('refreshed', { detail: { fileCount }, bubbles: true })
    }

    isLoaded() {
        return this.#treeRendered
    }

    async onLoad() {
        await this.playgroundOutlet.onLoad()

        if (this.#treeRendered) {
            return Promise.resolve()
        }

        return new Promise(resolve => {
            this.element.addEventListener('workspace:tree-rendered', resolve, { once: true })
        })
    }

    #renderTree(tree) {
        if (!this.hasTreeTarget) {
            return
        }

        const fileCount = this.#countFiles(tree)

        if (fileCount === 0) {
            this.#showEmptyState()
            return
        }

        this.treeTarget.innerHTML = this.#renderFileTree(tree)
        this.#treeRendered = true

        if (this.hasEmptyStateTarget) {
            this.emptyStateTarget.style.display = 'none'
        }

        const folderCount = this.#countFolders(tree)
        this.dispatch('tree-rendered', { detail: { fileCount, folderCount }, bubbles: true })
    }

    #buildFileTree(files) {
        const tree = {}

        for (const file of files) {
            const parts = file.path.split('/').filter(p => p)
            let current = tree

            for (let i = 0; i < parts.length; i++) {
                const part = parts[i]
                const isLast = i === parts.length - 1

                if (!current[part]) {
                    current[part] = {
                        name: part,
                        path: isLast ? file.path : parts.slice(0, i + 1).join('/'),
                        type: isLast ? file.type : 'directory',
                        children: {}
                    }
                }

                if (!isLast) {
                    current = current[part].children
                }
            }
        }

        return tree
    }

    #renderFileTree(tree, level = 0) {
        let html = '<ul class="file-tree">'

        const entries = Object.values(tree).sort((a, b) => {
            if (a.type === 'directory' && b.type !== 'directory') return -1
            if (a.type !== 'directory' && b.type === 'directory') return 1
            return a.name.localeCompare(b.name)
        })

        for (const entry of entries) {
            const indent = level * 16

            if (entry.type === 'directory') {
                html += `
                    <li class="file-tree-item directory" style="padding-left: ${indent}px">
                        <img src="${this.folderIconValue}" class="icon" width="16" height="16" alt="">
                        <span>${entry.name}</span>
                    </li>
                `

                if (entry.children && Object.keys(entry.children).length > 0) {
                    html += this.#renderFileTree(entry.children, level + 1)
                }
            } else {
                html += `
                    <li class="file-tree-item file clickable" style="padding-left: ${indent}px"
                        data-action="click->playground#previewFile"
                        data-file-path="${entry.path}">
                        <img src="${this.fileIconValue}" class="icon" width="16" height="16" alt="">
                        <span>${entry.name}</span>
                    </li>
                `
            }
        }

        html += '</ul>'
        return html
    }

    #updateSelectedState() {
        if (!this.hasTreeTarget) {
            return
        }

        const items = this.treeTarget.querySelectorAll('.file-tree-item.file')

        items.forEach(item => {
            const itemPath = item.dataset.filePath

            if (itemPath === this.#selectedFile) {
                item.classList.add('selected')
            } else {
                item.classList.remove('selected')
            }
        })
    }

    #showEmptyState() {
        if (this.hasTreeTarget) {
            this.treeTarget.innerHTML = ''
        }

        if (this.hasEmptyStateTarget) {
            this.emptyStateTarget.style.display = 'block'
        }
    }

    #countFiles(tree) {
        let count = 0

        for (const entry of Object.values(tree)) {
            if (entry.type === 'file') {
                count++
            } else if (entry.children) {
                count += this.#countFiles(entry.children)
            }
        }

        return count
    }

    #countFolders(tree) {
        let count = 0

        for (const entry of Object.values(tree)) {
            if (entry.type === 'directory') {
                count++

                if (entry.children) {
                    count += this.#countFolders(entry.children)
                }
            }
        }

        return count
    }
}
