package systemfolders

import "github.com/codecypress/go-ancillary-utils/miscellaneous"

func GetSystemFoldersErrorReportsPath() string {
	return miscellaneous.GetWorkingDir() + "/.system_folders/error_reports/"
}

func GetSystemFoldersTempCertsPath() string {
	return miscellaneous.GetWorkingDir() + "/.system_folders/temp_certs/"
}

func GetSystemFoldersCertificatesPath() string {
	return miscellaneous.GetWorkingDir() + "/.system_folders/certificates/"
}

func GetSystemFoldersAttachmentsPath() string {
	return miscellaneous.GetWorkingDir() + "/.system_folders/attachments/"
}

func GetSystemFoldersReportsPath() string {
	return miscellaneous.GetWorkingDir() + "/.system_folders/reports/"
}

func GetSystemFoldersChapterAvatarsPath() string {
	return miscellaneous.GetWorkingDir() + "/.system_folders/chapter_avatars/"
}

func GetSystemFoldersTopicAttachmentsPath() string {
	return miscellaneous.GetWorkingDir() + "/.system_folders/topic_attachments/"
}
