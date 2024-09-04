import logging
import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.userstate import OrderedListStateSpec


class MyDoFn(beam.DoFn):
    ORDERED_LIST_INDEX = OrderedListStateSpec('my_ordered_list', StrUtf8Coder())

    def process(
            self,
            element,
            ordered_list=beam.DoFn.StateParam(ORDERED_LIST_INDEX),
            **kwargs
    ):

        ordered_list.add((1, "a"))
        ordered_list.add((2, "b"))
        ordered_list.add((8, "c"))
        ordered_list.commit() # force to write to runner
        print("Step 0:", list(ordered_list.read()))

        ordered_list.add((2, "aa"))
        ordered_list.add((5, "bb"))
        ordered_list.add((9, "cc"))
        print("Step 1:", list(ordered_list.read()))

        ordered_list.clear_range(1, 7)
        print("Step 2:", list(ordered_list.read()))

        iter_before_add_and_remove = ordered_list.read()
        ordered_list.add((100, "i want to be the only one"))
        ordered_list.clear_range(8, 10)
        print("Ordered list after modification:", list(ordered_list.read()))
        print("Using iterator before modification",
            list(iter_before_add_and_remove))


def run():
    pipeline_options = PipelineOptions(None)

    with beam.Pipeline(options=pipeline_options) as p:
        _ = (
                p | "Create elements" >> beam.Create(["Hello", ])
                | "Add dummy key" >> beam.Map(lambda x: (1, x))
                | "Run my dofn" >> beam.ParDo(MyDoFn()))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
