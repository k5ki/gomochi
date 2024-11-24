package main

import (
	"context"
	"encoding/json"
	"fmt"
	"gomochi"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
)

var (
	AwsRegion string
	QueueName string

	awscfg aws.Config
)

func init() {
	AwsRegion = os.Getenv("AWS_REGION")
	QueueName = os.Getenv("QUEUE_NAME")

	var err error
	awscfg, err = awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(AwsRegion))
	if err != nil {
		panic(err)
	}
}

type Task struct {
	ID   uuid.UUID
	Text string
}

func main() {
	if err := createSampleTasks(); err != nil {
		panic(err)
	}

	mochi := gomochi.NewGomochi(
		map[string]gomochi.WorkerID{
			"aaa": "worker-1",
			"bbb": "worker-2",
			"ccc": "worker-2",
		},
		gomochi.NewSimpleHandlerBuilder(func(id gomochi.WorkerID, task *Task) error {
			fmt.Printf("worker: %s, task.ID: %s, task.Text: %s\n", id, task.ID, task.Text)
			time.Sleep(1 * time.Second)
			return nil
		}),
		awscfg,
		QueueName,
	)

	done := make(chan struct{})
	errChan := make(chan error)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		mochi.Run(done, errChan)
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigs:
		fmt.Println("signal received")
		break
	case err := <-errChan:
		fmt.Printf("error: %v\n", err)
		break
	}
	done <- struct{}{}
	wg.Wait()
}

func createSampleTasks() error {
	mkTasks := func(ctx context.Context, messageGroupId string) error {
		client := sqs.NewFromConfig(awscfg)

		for i := 0; i < 10; i++ {
			id, _ := uuid.NewV7()
			message := Task{
				ID:   id,
				Text: fmt.Sprintf("%s:%d", messageGroupId, i),
			}
			msg, _ := json.Marshal(message)

			_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:               aws.String(QueueName),
				MessageGroupId:         aws.String(messageGroupId),
				MessageBody:            aws.String(string(msg)),
				MessageDeduplicationId: aws.String(id.String()),
			})
			if err != nil {
				return err
			}
		}
		return nil
	}

	var wg sync.WaitGroup
	for _, messageGroupId := range []string{"aaa", "bbb", "ccc"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := mkTasks(context.Background(), messageGroupId); err != nil {
				panic(err)
			}
		}()
	}

	wg.Wait()

	return nil
}
