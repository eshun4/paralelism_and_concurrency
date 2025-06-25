import time

from yaml_reader import YamlPipelineExecutor



def main():
    pipeline_location = 'pipelines/wiki_yahoo_scrapper_pipeline.yaml'
    yamlPipelineExecutor = YamlPipelineExecutor(pipeline_location=pipeline_location)

    scraper_start_time = time.time()  # <-- move this up
    yamlPipelineExecutor.process_pipeline()
    end_time = time.time()

    print(f"✅ Finished in {round(end_time - scraper_start_time, 1)} seconds")

        

if __name__ == "__main__":
    main()