import time  # Import the time module for measuring execution duration

from yaml_reader import YamlPipelineExecutor  # Import the YamlPipelineExecutor class from yaml_reader module


def main():
    """
    Main function to execute the YAML pipeline and measure its execution time.
    """
    pipeline_location = 'pipelines/wiki_yahoo_scrapper_pipeline.yaml'  # Path to the YAML pipeline configuration file
    scraper_start_time = time.time()  # Record the start time before pipeline execution
    yamlPipelineExecutor = YamlPipelineExecutor(pipeline_location=pipeline_location)  # Initialize the pipeline executor with the YAML file

    # yamlPipelineExecutor.process_pipeline()  # (Commented out) Alternative method to process the pipeline
    yamlPipelineExecutor.start()  # Start the pipeline execution
    end_time = time.time()  # Record the end time after pipeline execution

    print(f"✅ Finished in {round(end_time - scraper_start_time, 1)} seconds")  # Print the total execution time


if __name__ == "__main__":
    main()  # Run the main function if this script is executed directly