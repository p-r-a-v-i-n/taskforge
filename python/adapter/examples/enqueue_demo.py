from taskforge import TaskforgeApp


def main() -> None:
    app = TaskforgeApp(broker_url="redis://127.0.0.1:6379")

    @app.task()
    def send_email(to: str, subject: str) -> None:
        # This function is not executed locally; it is a declaration only.
        pass

    handle = send_email.delay("user@example.com", "Hi from Taskforge")
    print(f"Enqueued task {handle.id}")


if __name__ == "__main__":
    main()
