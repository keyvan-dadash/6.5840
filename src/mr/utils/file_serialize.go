package utils

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
)

func FileToBase64(filePath string) (string, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file: %v", err)
	}

	base64String := base64.StdEncoding.EncodeToString(data)

	return base64String, nil
}

func Base64ToFile(base64String, outputFilePath string) error {
	data, err := base64.StdEncoding.DecodeString(base64String)
	if err != nil {
		return fmt.Errorf("failed to decode base64 string: %v", err)
	}

	err = ioutil.WriteFile(outputFilePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}
