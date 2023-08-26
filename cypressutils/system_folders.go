package cypressutils

func GetSystemFoldersErrorReportsPath() string {
	return GetWorkingDir() + "/.system_folders/error_reports/"
}

func GetSystemFoldersTempCertsPath() string {
	return GetWorkingDir() + "/.system_folders/temp_certs/"
}

func GetSystemFoldersCertificatesPath() string {
	return GetWorkingDir() + "/.system_folders/certificates/"
}

func GetSystemFoldersAttachmentsPath() string {
	return GetWorkingDir() + "/.system_folders/attachments/"
}

func GetSystemFoldersReportsPath() string {
	return GetWorkingDir() + "/.system_folders/reports/"
}

func GetSystemFoldersChapterAvatarsPath() string {
	return GetWorkingDir() + "/.system_folders/chapter_avatars/"
}

func GetSystemFoldersTopicAttachmentsPath() string {
	return GetWorkingDir() + "/.system_folders/topic_attachments/"
}
