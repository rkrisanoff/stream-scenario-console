<script lang="ts">
	import type { SidebarEvents, SidebarProps, UseCaseListItem, UseCaseListValue } from "./types";
	import { Gradio } from "@gradio/utils";
	import { Block } from "@gradio/atoms";
	import { StatusTracker } from "@gradio/statustracker";
	import Sortable from "sortablejs";
	import { onMount } from "svelte";

	const props = $props();
	const gradio = new Gradio<SidebarEvents, SidebarProps>(props);

	const container = true;

	let listEl: HTMLUListElement | undefined;
	let sortable: Sortable | null = null;
	let editingId = $state("");
	let editingName = $state("");
	let renameInputEl = $state<HTMLInputElement | undefined>();

	function normalizeValue(raw: UseCaseListValue | null | undefined): UseCaseListValue {
		if (!raw || typeof raw !== "object") {
			return { items: [], selected_id: "" };
		}
		const items = Array.isArray(raw.items)
			? raw.items
					.filter((item): item is UseCaseListItem => Boolean(item && item.id))
					.map((item) => ({ id: String(item.id), name: String(item.name ?? "") }))
			: [];
		return { items, selected_id: String(raw.selected_id ?? "") };
	}

	function emitChange(next: UseCaseListValue): void {
		gradio.update({ value: next });
		gradio.dispatch("change", next);
	}

	function selectItem(item: UseCaseListItem, index: number): void {
		if (editingId) {
			return;
		}
		const current = normalizeValue(gradio.props.value);
		if (current.selected_id === item.id) {
			return;
		}
		const next = { items: current.items, selected_id: item.id };
		emitChange(next);
		gradio.dispatch("select", { value: item.id, index });
	}

	function startRename(item: UseCaseListItem, event: MouseEvent): void {
		event.stopPropagation();
		editingId = item.id;
		editingName = item.name;
		queueMicrotask(() => {
			renameInputEl?.focus();
			renameInputEl?.select();
		});
	}

	function commitRename(item: UseCaseListItem): void {
		const trimmed = editingName.trim();
		editingId = "";
		if (!trimmed || trimmed === item.name) {
			return;
		}
		const current = normalizeValue(gradio.props.value);
		const items = current.items.map((row) =>
			row.id === item.id ? { ...row, name: trimmed } : row,
		);
		emitChange({ items, selected_id: current.selected_id });
	}

	function cancelRename(): void {
		editingId = "";
	}

	function handleRenameKeydown(event: KeyboardEvent, item: UseCaseListItem): void {
		if (event.key === "Enter") {
			event.preventDefault();
			commitRename(item);
		}
		if (event.key === "Escape") {
			event.preventDefault();
			cancelRename();
		}
	}

	function deleteItem(item: UseCaseListItem, index: number, event: MouseEvent): void {
		event.stopPropagation();
		const current = normalizeValue(gradio.props.value);
		if (current.items.length <= 1) {
			return;
		}
		if (!window.confirm(`Delete "${item.name}"?`)) {
			return;
		}
		const items = current.items.filter((row) => row.id !== item.id);
		let selected_id = current.selected_id;
		if (selected_id === item.id) {
			const nextIndex = Math.min(index, items.length - 1);
			selected_id = items[nextIndex]?.id ?? "";
		}
		emitChange({ items, selected_id });
	}

	function addItem(): void {
		gradio.dispatch("add");
	}

	function readOrderFromDom(): UseCaseListItem[] {
		if (!listEl) {
			return normalizeValue(gradio.props.value).items;
		}
		const byId = new Map(normalizeValue(gradio.props.value).items.map((item) => [item.id, item]));
		return [...listEl.querySelectorAll<HTMLElement>("[data-id]")]
			.map((element) => byId.get(element.dataset.id ?? ""))
			.filter((item): item is UseCaseListItem => Boolean(item));
	}

	function mountSortable(): void {
		if (!listEl || sortable) {
			return;
		}
		sortable = Sortable.create(listEl, {
			handle: ".drag-handle",
			animation: 150,
			onEnd: () => {
				const current = normalizeValue(gradio.props.value);
				const items = readOrderFromDom();
				emitChange({ items, selected_id: current.selected_id });
			},
		});
	}

	onMount(() => {
		mountSortable();
		return () => {
			sortable?.destroy();
			sortable = null;
		};
	});

	$effect(() => {
		normalizeValue(gradio.props.value);
		if (listEl && !sortable) {
			mountSortable();
		}
	});

	const value = $derived(normalizeValue(gradio.props.value));
	const allowDelete = $derived(Boolean(gradio.props.allow_delete));
	const showDeleteButton = $derived(allowDelete && value.items.length > 1);
</script>

<Block
	visible={gradio.shared.visible}
	elem_id={gradio.shared.elem_id}
	elem_classes={gradio.shared.elem_classes}
	{container}
	scale={gradio.shared.scale}
	min_width={gradio.shared.min_width}
>
	{#if gradio.shared.loading_status}
		<StatusTracker
			autoscroll={gradio.shared.autoscroll}
			i18n={gradio.i18n}
			{...gradio.shared.loading_status}
			on_clear_status={() => gradio.dispatch("clear_status", gradio.shared.loading_status)}
		/>
	{/if}

	<div class="use-case-sidebar wrap">
		<ul class="use-case-list" bind:this={listEl}>
			{#each value.items as item, index (item.id)}
				<li
					class="use-case-item"
					class:selected={item.id === value.selected_id}
					data-id={item.id}
				>
					<button type="button" class="drag-handle" aria-label="Reorder">≡</button>
					{#if editingId === item.id}
						<input
							class="rename-input"
							type="text"
							bind:this={renameInputEl}
							bind:value={editingName}
							onblur={() => commitRename(item)}
							onkeydown={(event) => handleRenameKeydown(event, item)}
						/>
					{:else}
						<button type="button" class="name-button" onclick={() => selectItem(item, index)}>
							{item.name}
						</button>
					{/if}
					<div class="item-actions">
						<button
							type="button"
							class="action-button"
							aria-label="Rename"
							title="Rename"
							onclick={(event) => startRename(item, event)}
						>
							✎
						</button>
						{#if allowDelete}
							<button
								type="button"
								class="action-button danger"
								aria-label="Delete"
								title="Delete"
								disabled={!showDeleteButton}
								onclick={(event) => deleteItem(item, index, event)}
							>
								🗑
							</button>
						{/if}
					</div>
				</li>
			{/each}
		</ul>
		<button type="button" class="add-button" onclick={addItem}>+ Add use case</button>
	</div>
</Block>

<style>
	.use-case-sidebar {
		display: flex;
		flex-direction: column;
		gap: 6px;
	}

	.use-case-list {
		list-style: none;
		margin: 0;
		padding: 0;
		display: flex;
		flex-direction: column;
		gap: 4px;
		max-height: 320px;
		overflow-y: auto;
	}

	.use-case-item {
		display: flex;
		align-items: center;
		gap: 4px;
		border: 1px solid var(--border-color-primary);
		border-radius: var(--radius-sm);
		background: var(--background-fill-secondary);
	}

	.use-case-item.selected {
		border-color: var(--color-accent);
		background: var(--background-fill-primary);
	}

	.drag-handle {
		cursor: grab;
		border: none;
		background: transparent;
		color: var(--body-text-color-subdued);
		padding: 8px 4px 8px 8px;
		font-size: 14px;
		line-height: 1;
		flex-shrink: 0;
	}

	.drag-handle:active {
		cursor: grabbing;
	}

	.name-button {
		flex: 1;
		min-width: 0;
		text-align: left;
		border: none;
		background: transparent;
		color: var(--body-text-color);
		padding: 8px 4px 8px 0;
		cursor: pointer;
		font: inherit;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.rename-input {
		flex: 1;
		min-width: 0;
		margin: 4px 0;
		padding: 4px 6px;
		border: 1px solid var(--color-accent);
		border-radius: var(--radius-sm);
		background: var(--background-fill-primary);
		color: var(--body-text-color);
		font: inherit;
	}

	.item-actions {
		display: flex;
		align-items: center;
		gap: 2px;
		padding-right: 4px;
		flex-shrink: 0;
		opacity: 0;
		transition: opacity 0.15s ease;
	}

	.use-case-item:hover .item-actions,
	.use-case-item.selected .item-actions {
		opacity: 1;
	}

	.action-button {
		border: none;
		background: transparent;
		color: var(--body-text-color-subdued);
		padding: 4px 6px;
		cursor: pointer;
		font-size: 13px;
		line-height: 1;
		border-radius: var(--radius-sm);
	}

	.action-button:hover:not(:disabled) {
		color: var(--body-text-color);
		background: var(--background-fill-secondary);
	}

	.action-button.danger:hover:not(:disabled) {
		color: var(--error-text-color, #c0392b);
	}

	.action-button:disabled {
		opacity: 0.35;
		cursor: not-allowed;
	}

	.add-button {
		display: flex;
		align-items: center;
		justify-content: center;
		gap: 6px;
		width: 100%;
		padding: 8px 10px;
		border: 1px dashed var(--border-color-primary);
		border-radius: var(--radius-sm);
		background: transparent;
		color: var(--body-text-color-subdued);
		cursor: pointer;
		font: inherit;
	}

	.add-button:hover {
		border-color: var(--color-accent);
		color: var(--body-text-color);
		background: var(--background-fill-secondary);
	}
</style>
