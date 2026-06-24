<script lang="ts">
	import type { UseCaseListValue } from "./types";

	export let value: UseCaseListValue;
	export let type: "gallery" | "table";
	export let selected = false;
</script>

<div class:table={type === "table"} class:gallery={type === "gallery"} class:selected>
	{value?.items?.find((item) => item.id === value.selected_id)?.name ?? ""}
</div>

<style>
	.gallery {
		padding: var(--size-1) var(--size-2);
	}
</style>
