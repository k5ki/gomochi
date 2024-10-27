package gomochi

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func sendError(err error, errChan chan<- error) {
	go func() {
		errChan <- err
	}()
}

type Handler[T any] interface {
	Handle(WorkerID, *T) error
}

type HandlerBuilder[T any] interface {
	Build() Handler[T]
}

type SimpleHandler[T any] struct {
	handle func(WorkerID, *T) error
}

func (h *SimpleHandler[T]) Handle(id WorkerID, task *T) error {
	return h.handle(id, task)
}

type SimpleHandlerBuilder[T any] struct {
	handle func(WorkerID, *T) error
}

func (b *SimpleHandlerBuilder[T]) Build() Handler[T] {
	return &SimpleHandler[T]{handle: b.handle}
}

func NewSimpleHandlerBuilder[T any](handle func(WorkerID, *T) error) HandlerBuilder[T] {
	return &SimpleHandlerBuilder[T]{handle: handle}
}

type WorkerID string

type worker[T any] struct {
	id      WorkerID
	wQueue  chan *T
	handler Handler[T]
	errChan chan<- error
}

func newWorker[T any](id WorkerID, wQueue chan *T, errChan chan<- error, handler Handler[T]) *worker[T] {
	return &worker[T]{
		id:      id,
		wQueue:  wQueue,
		errChan: errChan,
		handler: handler,
	}
}

func (w *worker[T]) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			select {
			case task := <-w.wQueue:
				err := w.handler.Handle(w.id, task)
				if err != nil {
					sendError(err, w.errChan)
				}
			default:
				return
			}
			return
		case task := <-w.wQueue:
			err := w.handler.Handle(w.id, task)
			if err != nil {
				sendError(err, w.errChan)
			}
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

type dispatcher[T any] struct {
	wQueues                map[WorkerID]chan *T
	messagesGroupIDMapping map[string]WorkerID
	client                 *sqs.Client
	queueName              string
	errChan                chan<- error
}

func newDispatcher[T any](wQeueus map[WorkerID]chan *T, messagesGroupIDMapping map[string]WorkerID, client *sqs.Client, queueName string, errChan chan<- error) *dispatcher[T] {
	return &dispatcher[T]{
		wQueues:                wQeueus,
		messagesGroupIDMapping: messagesGroupIDMapping,
		client:                 client,
		queueName:              queueName,
		errChan:                errChan,
	}
}

func (d *dispatcher[T]) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			d.run(ctx)
		}
	}
}

func (d *dispatcher[T]) run(_ context.Context) {
	res, err := d.client.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(d.queueName),
		AttributeNames: []types.QueueAttributeName{
			"MessageGroupId",
		},
		MaxNumberOfMessages: 2,
	})
	if err != nil {
		sendError(err, d.errChan)
		return
	}

	if len(res.Messages) == 0 {
		fmt.Println("dispatcher sleep(1)")
		time.Sleep(1 * time.Second)
		return
	}

	countSended := 0
	for _, msg := range res.Messages {
		id, ok := d.messagesGroupIDMapping[msg.Attributes["MessageGroupId"]]
		if !ok {
			err := fmt.Errorf("MessageGroupId %q is not found in mapping", msg.Attributes["MessageGroupId"])
			sendError(err, d.errChan)
		}

		var task T
		if err := json.Unmarshal([]byte(*msg.Body), &task); err != nil {
			sendError(err, d.errChan)
			return
		}

		sended := false
		select {
		case d.wQueues[id] <- &task:
			sended = true
			countSended++
		default:
		}

		if sended {
			_, err := d.client.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(d.queueName),
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				sendError(err, d.errChan)
			}
		} else {
			_, err := d.client.ChangeMessageVisibility(context.TODO(), &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          aws.String(d.queueName),
				ReceiptHandle:     msg.ReceiptHandle,
				VisibilityTimeout: 0,
			})
			if err != nil {
				sendError(err, d.errChan)
			}
		}
	}

	if countSended == 0 {
		fmt.Println("dispatcher sleep(2)")
		time.Sleep(1 * time.Second)
	}
}

type Gomochi[T any] struct {
	workerIDs             []WorkerID
	messageGroupIDMapping map[string]WorkerID
	handlerBuilder        HandlerBuilder[T]
	awscfg                aws.Config
	queueName             string
}

func NewGomochi[T any](messageGroupIDMapping map[string]WorkerID, builder HandlerBuilder[T], awscfg aws.Config, queueName string) *Gomochi[T] {
	ids := make([]WorkerID, 0, len(messageGroupIDMapping))
	for id := range maps.Values(messageGroupIDMapping) {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	return &Gomochi[T]{
		workerIDs:             ids,
		messageGroupIDMapping: messageGroupIDMapping,
		handlerBuilder:        builder,
		awscfg:                awscfg,
		queueName:             queueName,
	}
}

func (m *Gomochi[T]) Run(done <-chan struct{}, errChan chan<- error) {
	wQueues := make(map[WorkerID]chan *T)

	workers := make([]*worker[T], 0)
	for _, id := range m.workerIDs {
		wQueues[id] = make(chan *T, 10)
		worker := newWorker(id, wQueues[id], errChan, m.handlerBuilder.Build())
		workers = append(workers, worker)
	}

	dispatcher := newDispatcher(
		wQueues,
		m.messageGroupIDMapping,
		sqs.NewFromConfig(m.awscfg),
		m.queueName,
		errChan,
	)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}

	for _, worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Run(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatcher.Run(ctx)
	}()

	<-done
	cancel()
	wg.Wait()
}
