# ruff: noqa: ANN101, T201
"""Google Drive helper to retrieve folder content and download files."""

import io
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from .auth import get_google_credentials

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class GoogleDriveService:
    """GoogleDriveService class."""

    def __init__(self, project_id: str, root_folder_id: str) -> None:
        """Initialise GoogleDriveService."""
        self.service = build("drive", "v3", credentials=get_google_credentials(project_id))
        self.root_folder_id = root_folder_id

    def __query(self, query: str) -> dict[str : list[dict[str:str]]]:
        return self.service.files().list(q=query, spaces="drive", fields="files(id, name, mimeType)").execute()

    def download_file(self, file_id: str, destination_path: Path) -> Path:
        """Download a file from Google Drive using its file ID.

        Args:
        ----
            file_id (str): ID of the file to download.
            destination_path (Path): Local path where the file will be saved.

        Returns:
        -------
            bool: Whether download has been successful.

        """
        try:
            request = self.service.files().get_media(fileId=file_id)
            file_data = io.FileIO(destination_path, mode="wb")
            downloader = MediaIoBaseDownload(file_data, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Downloading '{destination_path!s}' - {int(status.progress() * 100)}%")
            file_data.close()
        except Exception as e:  # noqa: BLE001
            print(str(e))
            return Path()
        return destination_path

    def find_folder_by_name_in_root(self, target_folder_name: str) -> dict[str:str]:
        """Search for a specific folder by name within a parent folder on Google Drive.

        Args:
        ----
            parent_folder_id (str): ID of the parent folder in Google Drive.
            target_folder_name (str): Name of the folder to find.

        Returns:
        -------
            dict: Folder details (ID, name, mimeType) if found, otherwise an empty dictionary.

        """
        query = f"'{self.root_folder_id}' in parents and name = '{target_folder_name}' and mimeType = '{FOLDER_MIME_TYPE}'"
        results = self.service.files().list(q=query, spaces="drive", fields="files(id, name, mimeType)").execute()

        folders = results.get("files", [])
        return folders[0] if folders else {}

    def list_folder_contents(self, folder_id: str) -> list[dict[str:str]]:
        """List content of folder based on its ID.

        Args:
        ----
            folder_id: ID of the folder in Google Drive.

        Returns:
        -------
            list[dict[str:str]]: List of files/folders within the folder.

        """
        query = f"'{folder_id}' in parents and mimeType != '{FOLDER_MIME_TYPE}'"
        results = self.__query(query)
        return results.get("files", [])

    def move_file(self, file_id: str, folder_name: str) -> bool:
        """Move file to a specified folder by updating its parent folder.

        Args:
        ----
            file_id (str): The ID of the file to be moved.
            folder_name (str): The name of the destination folder.

        Returns:
        -------
            bool: True if the file was moved successfully, False otherwise.

        """
        # Find the destination folder ID by name
        folder_id = self.find_folder_by_name_in_root(folder_name).get("id")
        if not folder_id:
            print(f"Folder '{folder_name}' not found.")
            return False

        # Retrieve the current parent folders of the file
        file_metadata = self.service.files().get(fileId=file_id, fields="parents").execute()
        previous_parents = ",".join(file_metadata.get("parents", []))

        # Move the file by updating the 'parents' field to the new folder ID and removing the old parents
        try:
            self.service.files().update(fileId=file_id, addParents=folder_id, removeParents=previous_parents, fields="id, parents").execute()
            return True
        except Exception as e:  # noqa: BLE001
            print(f"An error occurred while moving the file: {e}")
            return False

    def upload_file(self, file_path: Path, folder_name: str | None = None) -> bool:
        """Upload a file to a specified folder on Google Drive.

        Args:
        ----
            file_path (Path): The path to the file to upload.
            folder_name (str): The name of the destination folder.

        Returns:
        -------
            bool: True if the file was uploaded successfully, False otherwise.

        """
        if not folder_name:
            folder_id = self.root_folder_id
        else:
            folder_id = self.find_folder_by_name_in_root(folder_name).get("id")
            if not folder_id:
                print(f"Folder '{folder_name}' not found.")
                return False

        try:
            self.service.files().create(
                body={"name": file_path.name, "parents": [folder_id]},
                media_body=MediaFileUpload(file_path, resumable=True),
                fields="id",
            ).execute()
            return True
        except Exception as e:  # noqa: BLE001
            print(f"An error occurred while uploading the file: {e}")
            return False
