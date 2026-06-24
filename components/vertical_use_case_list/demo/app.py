
import gradio as gr
from gradio_verticalusecaselist import VerticalUseCaseList


initial_value = {
    "items": [
        {"id": "kafka-1", "name": "Kafka use case 1"},
        {"id": "kafka-2", "name": "Kafka use case 2"},
    ],
    "selected_id": "kafka-1",
}


with gr.Blocks() as demo:
    gr.Markdown("# Vertical use case list")
    sidebar = VerticalUseCaseList(value=initial_value, label="Use cases", elem_id="kafka-use-cases")
    output = gr.JSON(label="Value")
    sidebar.change(lambda value: value, [sidebar], [output])


if __name__ == "__main__":
    demo.launch()
