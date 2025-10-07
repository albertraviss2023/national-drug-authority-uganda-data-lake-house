# set your repo root (yours looks like this from earlier prompts)
REPO="/nda-uganda/national-drug-authority-uganda-data-lake-house"

# make sure the target directories exist
sudo mkdir -p "$REPO/data/inspectorate/inspection_booking_2425"
sudo mkdir -p "$REPO/data/inspectorate/licensing"
sudo mkdir -p "$REPO/data/dps/gcp"

# copy the Excel files from Downloads to the right folders (preserve case in names)
sudo cp "/home/ndauganda/Downloads/inspection_booking_2425.xlsx" "$REPO/data/inspectorate/inspection_booking_2425/"
sudo cp "/home/ndauganda/Downloads/Licensing_2024.xlsx"         "$REPO/data/inspectorate/licensing/"
sudo cp "/home/ndauganda/Downloads/Licensing_2025.xlsx"         "$REPO/data/inspectorate/licensing/"
sudo cp "/home/ndauganda/Downloads/GCP_BOOKING.xlsx" "$REPO/data/dps/gcp/"
# fix ownership back to your user (since we used sudo)
sudo chown -R "$USER:$USER" "$REPO/data/inspectorate"

# quick verification
ls -lh "$REPO/data/inspectorate/inspection_booking_2425"
ls -lh "$REPO/data/inspectorate/licensing"
