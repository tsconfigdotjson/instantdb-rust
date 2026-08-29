import React, { useState } from "react";
import { init, id } from "@instantdb/react";

const APP_ID = import.meta.env.VITE_INSTANT_APP_ID;
const API = import.meta.env.VITE_INSTANT_API_URI || "http://localhost:8888";

const db = init({
  appId: APP_ID,
  apiURI: API,
  websocketURI: API.replace(/^http/, "ws") + "/runtime/session",
  devtool: false,
});

const room = db.room("todos", "main");

const styles = {
  page: {
    fontFamily: "ui-monospace, monospace",
    minHeight: "100vh",
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center",
    gap: 16,
    background: "#fafafa",
  },
  box: { border: "1px solid #ccc", width: 340, background: "white" },
  row: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    padding: "8px 10px",
    borderBottom: "1px solid #eee",
  },
  input: { border: "none", outline: "none", width: "100%", fontSize: 14 },
};

function App() {
  const { isLoading, error, data } = db.useQuery({
    todos: { $: { order: { serverCreatedAt: "asc" } } },
  });
  const { peers } = db.rooms.usePresence(room);
  const { user } = db.useAuth();
  const numUsers = 1 + Object.keys(peers).length;

  if (isLoading) return <div style={styles.page}>Loading…</div>;
  if (error) return <div style={styles.page}>Error: {error.message}</div>;

  const todos = data.todos || [];
  return (
    <div style={styles.page}>
      <div data-testid="online" style={{ fontSize: 12, color: "#888" }}>
        Users online: {numUsers}
      </div>
      <h2 style={{ letterSpacing: 2, color: "#bbb", fontSize: 40, margin: 0 }}>
        todos
      </h2>
      <div style={styles.box}>
        <TodoForm />
        <TodoList todos={todos} />
        <ActionBar todos={todos} />
      </div>
      <AuthSection user={user} />
      <div style={{ fontSize: 12, color: "#888" }}>
        Open another tab to see todos update in realtime!
      </div>
    </div>
  );
}

function addTodo(text) {
  db.transact(
    db.tx.todos[id()].update({ text, done: false, createdAt: Date.now() }),
  );
}

function toggleDone(todo) {
  db.transact(db.tx.todos[todo.id].update({ done: !todo.done }));
}

function deleteTodo(todo) {
  db.transact(db.tx.todos[todo.id].delete());
}

function deleteCompleted(todos) {
  db.transact(todos.filter((t) => t.done).map((t) => db.tx.todos[t.id].delete()));
}

function TodoForm() {
  return (
    <div style={styles.row}>
      <form
        style={{ width: "100%" }}
        onSubmit={(e) => {
          e.preventDefault();
          const input = e.target.elements.text;
          if (input.value.trim()) addTodo(input.value.trim());
          input.value = "";
        }}
      >
        <input
          style={styles.input}
          name="text"
          data-testid="new-todo"
          autoFocus
          placeholder="What needs to be done?"
          type="text"
        />
      </form>
    </div>
  );
}

function TodoList({ todos }) {
  return (
    <div data-testid="todo-list">
      {todos.map((todo) => (
        <div key={todo.id} style={styles.row} data-testid="todo-item">
          <input
            type="checkbox"
            checked={!!todo.done}
            onChange={() => toggleDone(todo)}
          />
          <span
            style={{
              flex: 1,
              textDecoration: todo.done ? "line-through" : "none",
            }}
          >
            {todo.text}
          </span>
          <button onClick={() => deleteTodo(todo)} title="delete">
            ⌫
          </button>
        </div>
      ))}
    </div>
  );
}

function ActionBar({ todos }) {
  return (
    <div style={{ ...styles.row, fontSize: 12, justifyContent: "space-between" }}>
      <span data-testid="remaining">
        Remaining todos: {todos.filter((t) => !t.done).length}
      </span>
      <button style={{ fontSize: 12 }} onClick={() => deleteCompleted(todos)}>
        Delete Completed
      </button>
    </div>
  );
}

function AuthSection({ user }) {
  const [email, setEmail] = useState("");
  const [codeSent, setCodeSent] = useState(false);
  const [code, setCode] = useState("");
  const [err, setErr] = useState(null);

  if (user) {
    return (
      <div style={{ fontSize: 12 }} data-testid="auth">
        Signed in as <b data-testid="user-email">{user.email}</b>{" "}
        <button onClick={() => db.auth.signOut()}>Sign out</button>
      </div>
    );
  }
  return (
    <div style={{ fontSize: 12 }} data-testid="auth">
      {!codeSent ? (
        <form
          onSubmit={async (e) => {
            e.preventDefault();
            try {
              await db.auth.sendMagicCode({ email });
              setCodeSent(true);
              setErr(null);
            } catch (e) {
              setErr(e.body?.message || e.message);
            }
          }}
        >
          <input
            data-testid="email"
            placeholder="email for magic code"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
          <button type="submit">Send code</button>
        </form>
      ) : (
        <form
          onSubmit={async (e) => {
            e.preventDefault();
            try {
              await db.auth.signInWithMagicCode({ email, code });
              setErr(null);
            } catch (e) {
              setErr(e.body?.message || e.message);
            }
          }}
        >
          <input
            data-testid="code"
            placeholder="code"
            value={code}
            onChange={(e) => setCode(e.target.value)}
          />
          <button type="submit">Verify</button>
        </form>
      )}
      {err && <div style={{ color: "red" }}>{err}</div>}
    </div>
  );
}

export default App;
