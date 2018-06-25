build:
	GOOS=linux GOARCH=amd64 go build

upload:
	scp pdm dmishin@oasis-bobcat:/home/dmishin/
