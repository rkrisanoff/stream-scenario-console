import type { LoadingStatus, SelectData } from "@gradio/utils";

export type UseCaseListItem = {
	id: string;
	name: string;
};

export type UseCaseListValue = {
	items: UseCaseListItem[];
	selected_id: string;
};

export interface SidebarProps {
	value: UseCaseListValue;
	allow_delete: boolean;
}

export interface SidebarEvents {
	change: UseCaseListValue;
	select: SelectData;
	add: never;
	clear_status: LoadingStatus;
}
