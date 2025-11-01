# === AtmoSeer Makefile ===

# Diretório base para os dados
DATA_DIR=data

# Diretório base para o código-fonte
SRC_DIR=src

# ========== Surfaces stations - CEMADEN ========================
cemaden_retrieve_data:
	PYTHONPATH=src python3 src/surface_stations/retrieve_data_cemaden.py \
	  $(if $(IBGE),--ibge $(IBGE)) \
	  $(if $(START),--inicio $(START)) \
	  $(if $(END),--fim $(END)) \
	  $(if $(EMAIL),--email $(EMAIL)) \
	  $(if $(SENHA),--senha $(SENHA)) \
	  $(if $(ESTACAO),--estacao $(ESTACAO))

cemaden_retrieve_ws:
	PYTHONPATH=src python3 src/surface_stations/retrieve_ws_cemaden.py \
	  $(if $(ESTADO),-e $(ESTADO)) \
	  $(if $(IDESTACAO),-c $(IDESTACAO)) \
	  $(if $(INICIO),-start $(INICIO)) \
	  $(if $(FIM),-end $(FIM)) \
	  $(if $(SAIDA),-o $(SAIDA)) \

cemaden_mapa:
	PYTHONPATH=src python3 src/surface_stations/cemaden_mapa.py 

cemaden_mapa_chuva:
	PYTHONPATH=src python3 src/surface_stations/cemaden_mapa_chuva.py

# ========== Surfaces stations - INMET ========================
inmet_retrieve_ws:
	PYTHONPATH=src python3 src/surface_stations/retrieve_ws_inmet.py \
	  $(if $(API_TOKEN),-t $(API_TOKEN)) \
	  $(if $(STATION_ID),-s $(STATION_ID)) \
	  $(if $(BEGIN_YEAR),-b $(BEGIN_YEAR)) \
	  $(if $(END_YEAR),-e $(END_YEAR))
# Example usage:
# make inmet_retrieve_ws API_TOKEN=your_token STATION_ID=A601 BEGIN_YEAR=2020 END_YEAR=2023

surface_stations_preprocess:
	PYTHONPATH=src python3 src/surface_stations/preprocess.py \
	  $(if $(STATION_ID),--station_id $(STATION_ID)) \
	  $(if $(SYSTEM),--station_system $(SYSTEM)) 
# Example usage:
# make surface_stations_preprocess STATION_ID=A601 SYSTEM=INMET	


# === GOES-16 Downloader/Cropper ===
goes16-download-crop:
	PYTHONPATH=src python src/goes16/goes16_download_crop.py \
	  --start_date $(START) \
	  --end_date $(END) \
	  --channel $(CHANNEL) \
	  --crop_dir $(DIR) \
	  --spatial_resolution 0.1 \
	  --vars CMI

# === GOES-16 Feature Extractor ===
goes16-features:
	PYTHONPATH=src python src/goes16/main_goes16_features.py $(FEATS)

sumare-crop-images:
	PYTHONPATH=src python3 src/sumare_radar/crop_image.py

# Validates a GOES-16 feature based on the specified channels
validate-feature:
	@echo "Validating feature '$(FEAT)' with channels $(CANAIS)"
	PYTHONPATH=src python src/goes16/validate_features.py --feature $(FEAT) --canais $(CANAIS)

# Summarize GOES-16 feature extraction log files
analyze-logs:
	@echo "Analyzing log files in src/goes16/features/"
	@PYTHONPATH=src python src/goes16/features/analyze_logs.py

# === GPM Download/Cropper ===
gpm-download-crop:
	PYTHONPATH=src python src/gpm/gpm_download_crop.py \
		--begin_date "$(BEGIN)" \
		--end_date "$(END)" \
		--user "$(USER)" \
		--pwd "$(PWD)" \
		$(DEBUG) \
		$(IGNORED)


# === Alerta Rio Parser (placeholder) ===
alerta-rio-process:
	PYTHONPATH=$(SRC_DIR) python $(SRC_DIR)/surface_stations/alerta_rio_parser.py


# === Clean Outputs ===
clean:
	rm -rf $(DATA_DIR)/goes16/features/*

.PHONY: format lint lint-fix pre-commit-install

format:
	ruff format src tests
	black src tests

lint:
	ruff check src tests

lint-fix:
	ruff check src tests --fix
	black src tests

pre-commit-install:
	pre-commit install
