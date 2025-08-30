# === AtmoSeer Makefile ===

# Diretório base para os dados
DATA_DIR=data

# Diretório base para o código-fonte
SRC_DIR=src

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