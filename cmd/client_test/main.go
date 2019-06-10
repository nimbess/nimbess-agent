package main

import (
	"flag"
	"fmt"
	dockerTypes "github.com/docker/docker/api/types"
	dockerCTypes "github.com/docker/docker/api/types/container"
	dockerNTypes "github.com/docker/docker/api/types/network"
	docker "github.com/docker/docker/client"
	"io"
	"os"
	"strings"
	"time"

	"github.com/nimbess/nimbess-agent/pkg/proto/cni"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const containerName = "vport_test"
const nimbessPort = 9111
const dockerImg = "docker.io/juamorous/ubuntu-ifconfig-ping"

func main() {
	cleanFlag := flag.Bool("clean", false, "Clean up test env")
	flag.Parse()

	if *cleanFlag {
		log.Info("Cleaning test env")
		// TODO implement
	}
	log.Info("Connecting to Nimbess")
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", nimbessPort), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}

	defer conn.Close()
	cli, err := docker.NewClientWithOpts(docker.FromEnv)
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 2; i++ {
		containerID := fmt.Sprintf("%s%d", containerName, i)
		container, err := cli.ContainerInspect(context.Background(), containerID)
		if err != nil {
			log.Info("Container does not exist. Checking docker image...")
			imgSum, err := cli.ImageList(context.Background(), dockerTypes.ImageListOptions{All: true})
			pull := true
			if err != nil {
				pull = true
				log.Error("Error listing images")
			} else {
			Loop:
				for _, img := range imgSum {
					for _, repoTag := range img.RepoTags {
						if strings.Contains(repoTag, "ubuntu-ifconfig-ping") {
							pull = false
							break Loop
						}
					}
				}
			}

			if pull == true {
				log.Info("Pulling docker image")
				r, err := cli.ImagePull(context.Background(), dockerImg,
					dockerTypes.ImagePullOptions{})
				if err != nil {
					log.Fatalf("Error pulling docker image, %v", err)
				} else {
					io.Copy(os.Stdout, r)
				}
			} else {
				log.Info("Docker image exists. Creating container...")
			}
			dConfig := &dockerCTypes.Config{
				OpenStdin:       true,
				Image:           dockerImg,
				NetworkDisabled: false,
				Cmd:             []string{"/usr/bin/tail", "-f", "/dev/null"},
			}
			hConfig := &dockerCTypes.HostConfig{
				NetworkMode: "none",
			}
			nConfig := &dockerNTypes.NetworkingConfig{
				EndpointsConfig: make(map[string]*dockerNTypes.EndpointSettings),
			}
			_, err = cli.ContainerCreate(context.Background(), dConfig, hConfig, nConfig, containerId)
			if err != nil {
				log.Fatalf("Failed to create container: %v", err)
			}
			log.Info("Container created.")
		}
		container, err = cli.ContainerInspect(context.Background(), containerId)

		if err != nil {
			log.Fatalf("Container does not exist: %v", err)
		}

		if !container.State.Running {
			log.Info("Starting container...")
			err = cli.ContainerStart(context.Background(), containerId, dockerTypes.ContainerStartOptions{})
			if err != nil {
				log.Fatalf("Unable to start container %s: %v", containerId, err)
			}
			time.Sleep(5 * time.Second)
		}
		container, err = cli.ContainerInspect(context.Background(), containerId)

		if err != nil {
			log.Fatalf("Container does not exist: %v", err)
		}
		log.Infof("container namespace: %s", container.NetworkSettings.SandboxKey)

		client := cni.NewRemoteCNIClient(conn)
		nwConf := &cni.CNIRequest_NetworkConfig{
			Name: "testNetwork",
		}
		testReq := &cni.CNIRequest{
			ContainerId:      containerId,
			InterfaceName:    "eth0",
			NetworkConfig:    nwConf,
			NetworkNamespace: container.NetworkSettings.SandboxKey,
		}

		res, err := client.Add(context.Background(), testReq)
		if err != nil || res.Error != "" || res.Result != 0 {
			log.Fatalf("Port Add failed: %v, %v", res, err)
		}
		log.Infof("Port added!: %s", res.Interfaces)
		log.Infof("Result is: %v", res)
	}
	/*
		dres, derr := client.Delete(context.Background(), testReq)
		if derr != nil || dres.Error !="" || dres.Result !=0 {
			log.Fatalf("Port Delete failed: %v, %v", dres, derr)
		}
		log.Infof("Port deleted!: %s", res.Interfaces)
	*/

}
