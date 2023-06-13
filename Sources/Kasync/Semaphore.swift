//
// DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
//
// Copyright (c) 2023 Andrey Kashaed. All rights reserved.
//
// The contents of this file are subject to the terms of the
// Common Development and Distribution License 1.0 (the "License").
// You may not use this file except in compliance with the License.
//
// You can obtain a copy of the License at
// https://opensource.org/licenses/CDDL-1.0 or LICENSE.txt.
//

import Foundation

public enum SemaphoreError: Error {
    case resetSemaphore
}

extension SemaphoreError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .resetSemaphore:
            return "Semaphore is reset"
        }
    }
}

public final class Semaphore: @unchecked Sendable {
    
    private let level: Int
    private var enabled: Bool
    private var counter: Int
    private var continuations: [CheckedContinuation<Void, Error>] = []
    private let lock = NSRecursiveLock()
    
    public init(level: Int, enabled: Bool = true) {
        self.level = level
        self.enabled = enabled
        self.counter = level
    }
    
    public func await(enabledAfterCancellation: Bool = false) async throws {
        if lock.withLock({ !enabled }) { return }
        try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Void, Error>, cancellation: Cancellation) -> Void in
            cancellation.onCancel = { [weak self] in
                self?.reset(enabled: enabledAfterCancellation)
            }
            self?.addContinuation(continuation)
            self?.dispatchContinuations()
        }
    }
    
    public func signal() {
        lock.withLock {
            if !enabled { return }
            counter += 1
            dispatchContinuations()
        }
    }
    
    public func reset(enabled: Bool, error: Error = SemaphoreError.resetSemaphore) {
        lock.withLock {
            self.enabled = enabled
            for continuation in continuations {
                continuation.resume(throwing: error)
            }
            continuations.removeAll()
            counter = level
        }
    }
    
    private func addContinuation(_ continuation: CheckedContinuation<Void, Error>) {
        lock.withLock {
            continuations.append(continuation)
        }
    }
    
    private func dispatchContinuations() {
        lock.withLock {
            while continuations.count > 0 && counter > 0 {
                let continuation = continuations.removeFirst()
                continuation.resume()
                counter -= 1
            }
        }
    }
    
}
